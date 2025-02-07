import subprocess
import psutil
import os
import csv
import pandas as pd
import psycopg2
import chardet
from psycopg2 import sql
from dotenv import load_dotenv
import glob

# 🔹 Carregar variáveis de ambiente
load_dotenv()

# 🔹 Configuração do Banco de Dados PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "cad")
DB_USER = os.getenv("DB_USER", "caduser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "cadpassword")

# 🔹 Diretório onde o CSV será salvo
CSV_DIRECTORY = os.getcwd()

# 🔹 Função para conectar ao banco de dados
def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

# 🔹 Função para verificar e encerrar processos relacionados ao `cad.jnlp`
def check_and_kill_cad_jnlp(tarefa):
    for process in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        try:
            if process.info['cmdline'] and any(tarefa in arg for arg in process.info['cmdline']):
                print(f"Processo encontrado: PID {process.info['pid']}, Nome: {process.info['name']}")
                os.kill(process.info['pid'], 9)
                print(f"Processo com PID {process.info['pid']} terminado.")
                return "continue"
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    print(f"Nenhum processo contendo '{tarefa}' foi encontrado.")
    return "continue"

def veryfy_if_is_running(process_name="cad.jnlp"):
    try:
        # Executa o comando 'ps aux' e obtém a saída
        output = subprocess.check_output(["ps", "aux"], text=True)
        # Verifica se o processo está na lista
        return process_name in output
    except subprocess.CalledProcessError:
        return False

# 🔹 Criar a tabela se ela não existir
def create_table():
    connection = get_db_connection()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS chamados (
            numero_chamada TEXT PRIMARY KEY,
            numero_reds TEXT,
            data_criacao TEXT,
            local_fato TEXT,
            latitude REAL,
            longitude REAL,
            natureza TEXT,
            unidade_responsavel TEXT,
            recursos_empenhados TEXT,
            alerta TEXT,
            destaque TEXT,
            envolve_autoridade TEXT,
            tipo_classificacao TEXT,
            situacao TEXT,
            data_situacao_atual TEXT,
            evento_associado TEXT
        )
        '''
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Tabela 'chamados' verificada/criada com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao criar tabela: {e}")
    finally:
        cursor.close()
        connection.close()

# 🔹 Função para sincronizar o CSV com o banco de dados

def sync_csv_to_database():
    CSV_DIRECTORY = os.getcwd()  # Usar o diretório corrente

    # Cria a tabela se ela não existir
    create_table()
    
    # Procurar arquivos CSV no diretório corrente
    csv_files = [file for file in os.listdir(CSV_DIRECTORY) if file.endswith(".csv")]
    if len(csv_files) != 1:
        print(f"⚠️ Esperava encontrar exatamente 1 arquivo CSV, mas encontrei: {len(csv_files)}")
        return
    
    csv_file_path = os.path.join(CSV_DIRECTORY, csv_files[0])
    print(f"📂 Arquivo CSV encontrado: {csv_file_path}")

    # Detectar encoding do arquivo CSV
    with open(csv_file_path, "rb") as f:
        encoding_detected = chardet.detect(f.read())['encoding']
    print(f"📂 Encoding detectado: {encoding_detected}")

    connection = get_db_connection()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()

        # Ler o CSV corretamente
        df = pd.read_csv(csv_file_path, sep=",", encoding=encoding_detected, dtype=str)

        print("Colunas disponíveis:", df.columns.tolist())

        # Remover espaços extras, normalizar e renomear colunas corretamente
        df.columns = df.columns.str.strip()  # Remove espaços em branco
        df.columns = df.columns.str.lower()  # Converte para minúsculas
        df.columns = df.columns.str.replace(" ", "_")  # Substitui espaços por "_"
        df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')  # Remove acentos

        # Mapeamento correto das colunas do CSV para o banco de dados
        colunas_corretas = {
            'no_chamada': 'numero_chamada',
            'no_reds': 'numero_reds',
            'data/hora_de_criacao': 'data_criacao',
            'local_do_fato': 'local_fato',
            'latitude__do_local': 'latitude',
            'longitude_do_local': 'longitude',
            'natureza': 'natureza',
            'unidade_responsavel': 'unidade_responsavel',
            'recursos_empenhados': 'recursos_empenhados',
            'alerta': 'alerta',
            'destaque': 'destaque',
            'envolve_autoridade': 'envolve_autoridade',
            'tipo_de_classificacao': 'tipo_classificacao',
            'situacao': 'situacao',
            'data/hora_da_situacao_atual': 'data_situacao_atual',
            'evento_associado': 'evento_associado'
        }

        # Aplicar renomeação para que os nomes correspondam ao banco de dados
        df.rename(columns=colunas_corretas, inplace=True)

        # Verificar se todas as colunas esperadas estão presentes
        print("Colunas após renomeação:", df.columns.tolist())

        # Se a coluna correta não existir, interrompa com erro
        if 'numero_chamada' not in df.columns:
            raise ValueError("Erro: A coluna 'numero_chamada' não foi encontrada no CSV!")

        # Remover duplicatas com base na coluna correta
        df.drop_duplicates(subset=['numero_chamada'], keep='first', inplace=True)

        # Conectar ao banco de dados
        connection = get_db_connection()
        cursor = connection.cursor()

        # Ajuste no SQL para buscar os registros corretamente
        cursor.execute("SELECT numero_chamada FROM chamados")
        registros_existentes = {row[0] for row in cursor.fetchall()}

        # Inserção e atualização ajustadas para usar os nomes corretos
        for _, row in df.iterrows():
            dados = tuple(row)
            if row['numero_chamada'] not in registros_existentes:
                cursor.execute('''
                    INSERT INTO chamados (
                        numero_chamada, numero_reds, data_criacao, local_fato, latitude, longitude,
                        natureza, unidade_responsavel, recursos_empenhados, alerta, destaque,
                        envolve_autoridade, tipo_classificacao, situacao, data_situacao_atual, evento_associado
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', dados)
                print(f"✅ Registro inserido: {row['numero_chamada']}")
            else:
                cursor.execute('''
                    UPDATE chamados SET
                        numero_reds = %s, data_criacao = %s, local_fato = %s, latitude = %s, longitude = %s,
                        natureza = %s, unidade_responsavel = %s, recursos_empenhados = %s, alerta = %s, destaque = %s,
                        envolve_autoridade = %s, tipo_classificacao = %s, situacao = %s, data_situacao_atual = %s, evento_associado = %s
                    WHERE numero_chamada = %s
                ''', dados[1:] + (row['numero_chamada'],))
                print(f"♻️ Registro atualizado: {row['numero_chamada']}")


        ##### Remover os registros que estão no Banco de Dados registros ausentes no Dataframe


        # Obter todos os registros presentes no CSV
        registros_no_csv = set(df['numero_chamada'])

        # Identificar registros que precisam ser removidos
        registros_para_remover = registros_existentes - registros_no_csv

        # Remover registros que estão no banco, mas não no CSV
        if registros_para_remover:
            cursor.execute(
                "DELETE FROM chamados WHERE numero_chamada IN %s",
                (tuple(registros_para_remover),)
            )
            print(f"🗑️ {len(registros_para_remover)} registros removidos do banco de dados.")
            
        connection.commit()

    
    except Exception as e:
        print(f"❌ Erro ao interagir com o banco de dados: {e}")
    
    finally:
        cursor.close()
        connection.close()


def merge_csv_files(input_folder, output_file):
    """
    Mescla todos os arquivos CSV de uma pasta em um único arquivo CSV, removendo duplicatas e lidando com valores ausentes.
    
    :param input_folder: Caminho da pasta onde estão os arquivos CSV.
    :param output_file: Nome do arquivo CSV de saída.
    """
    all_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    if not all_files:
        print("Nenhum arquivo CSV encontrado.")
        return
    
    df_list = []
    
    for file in all_files:
        file_path = os.path.join(input_folder, file)
        try:
            df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
            df_list.append(df)
        except Exception as e:
            print(f"Erro ao processar {file}: {e}")
    
    if not df_list:
        print("Nenhum arquivo válido para mesclar.")
        return
    
    merged_df = pd.concat(df_list, ignore_index=True)
    
    # Remover duplicatas com base no número de chamada
    if 'Nº chamada' in merged_df.columns:
        merged_df.drop_duplicates(subset=['Nº chamada'], keep='first', inplace=True)
    
    # Lidar com valores ausentes preenchendo com "Desconhecido" para strings e 0 para numéricos
    merged_df.fillna({
        'Nº chamada': 'Desconhecido',
        'Nº REDS': 'Desconhecido',
        'Data/hora de criação': 'Desconhecido',
        'Local do fato': 'Desconhecido',
        'Latitude  do local': 0,
        'Longitude do local': 0,
        'Natureza': 'Desconhecido',
        'Unidade Responsável': 'Desconhecido',
        'Recursos empenhados': 'Desconhecido',
        'Alerta': 'Desconhecido',
        'Destaque': 'Desconhecido',
        'Envolve autoridade': 'Desconhecido',
        'Tipo de classificação': 'Desconhecido',
        'Situação': 'Desconhecido',
        'Data/hora da situação atual': 'Desconhecido',
        'Evento associado': 'Desconhecido'
    }, inplace=True)
    
    # Salvar arquivo mesclado
    merged_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Arquivo mesclado salvo como: {output_file}")

def count_records(csv_file):
    """
    Conta o número de registros (linhas) em um arquivo CSV, excluindo o cabeçalho.
    
    :param csv_file: Caminho para o arquivo CSV.
    """
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')
        num_records = len(df)
        print(f"O arquivo '{csv_file}' contém {num_records} registros.")
        return num_records
    except Exception as e:
        print(f"Erro ao ler o arquivo {csv_file}: {e}")
        return None

def delete_all_files(folder_path):
    """
    Apaga todos os arquivos dentro de uma pasta especificada.
    
    :param folder_path: Caminho da pasta onde os arquivos serão apagados.
    """
    try:
        files = glob.glob(os.path.join(folder_path, "*"))  # Lista todos os arquivos
        for file in files:
            if os.path.isfile(file):
                os.remove(file)
        print(f"Todos os arquivos em '{folder_path}' foram apagados.")
    except Exception as e:
        print(f"Erro ao apagar arquivos na pasta {folder_path}: {e}")

if __name__ == "__main__":
    # create_table()
    # sync_csv_to_database()
    # Exemplo de uso
    input_folder = "/tmp/cliente-cad"  # Substitua pelo caminho real
    output_file = "arquivo_mesclado.csv"
    # merge_csv_files(input_folder, output_file)
    # count_records("arquivo_mesclado.csv")
    # delete_all_files('teste')
    # create_table()
