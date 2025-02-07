import psycopg2
from psycopg2 import sql
from api_lat_long import obter_coordenadas, temporizador_aleatorio
from dotenv import load_dotenv
import os


# 🔹 Carregar variáveis de ambiente
load_dotenv()

# 🔹 Configuração do Banco de Dados PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "cad")
DB_USER = os.getenv("DB_USER", "caduser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "cadpassword")

def atualizar_coordenadas():
    # Configurações do banco de dados
    DB_CONFIG = {
        'dbname': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'host': 'localhost',
        'port': DB_PORT
    }
    
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Selecionar registros onde latitude e longitude são 0
        query_select = sql.SQL("""
            SELECT numero_chamada, local_fato FROM sua_tabela
            WHERE latitude = 0 AND longitude = 0
        """)
        cursor.execute(query_select)
        registros = cursor.fetchall()
        
        for numero_chamada, local_fato in registros:
            try:
                # Obter coordenadas do endereço
                latitude, longitude = obter_coordenadas(local_fato)
                
                # Atualizar o registro com as novas coordenadas
                query_update = sql.SQL("""
                    UPDATE sua_tabela
                    SET latitude = %s, longitude = %s
                    WHERE numero_chamada = %s
                """)
                cursor.execute(query_update, (latitude, longitude, numero_chamada))
                conn.commit()
                print(f"Atualizado: {numero_chamada} -> ({latitude}, {longitude})")

                temporizador_aleatorio()
            
            except Exception as e:
                print(f"Erro ao processar {numero_chamada}: {e}")
        
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    atualizar_coordenadas()
