import subprocess
from pyautogui import write, press, hotkey, PAUSE, locateCenterOnScreen, click, size
from time import sleep
import pyperclip
import re
from dotenv import load_dotenv
import os
from utils import check_and_kill_cad_jnlp, count_records, sync_csv_to_database, merge_csv_files, delete_all_files, veryfy_if_is_running, create_table
import random

load_dotenv()

LOGIN = os.environ.get('LOGIN')
SENHA = os.environ.get('SENHA')

INTERVALO = 0.1
INTERVALO_SLOW = 0.3
INTERVALO_SLOW_SUPER = 0.5
# Controle se é a primeira vez ou não para não dar TABs a mais

ALL_DATA = []

CONTINUE_RESET = True


def abrir_cad():
    try:
        # Caminho para o script de execução do CAD
        caminho_script = "/home/cleytonfs/Documentos/CADBOT/index.sh"

        # Executa o script no terminal
        processo = subprocess.Popen(
            [caminho_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        print("Aplicação CAD foi iniciada.")
        return processo
    except Exception as e:
        print(f"Erro ao tentar abrir o CAD: {e}")



def dig_humano(frase):
    for char in frase:
        write(char, interval=random.uniform(0.1, 0.5))


def login_cad():
    try:
        # Espera de 15 segundos para o CAD abrir
        sleep(15)
        dig_humano(LOGIN)
        press('tab')
        dig_humano(SENHA)
        sleep(INTERVALO_SLOW)
        press('enter')
        sleep(8)
        # Atalho para pesquisa de chamadas
        hotkey('alt', 'c')
        for i in range(0, 3):
            press('down')
            sleep(INTERVALO_SLOW)
        press('enter')

    except Exception as e:
        print(f"Erro ao tentar abrir o CAD: {e}")

def reset_fields():
    # Recua até o botão 'Limpar Campos'
    for t in range(2):
        hotkey('shift', 'tab')
    sleep(INTERVALO_SLOW)
    press('enter')
    sleep(INTERVALO_SLOW)
    # Posiciona na seleção de COBs
    for t in range(3):
        hotkey('shift', 'tab')


def selecoes(op):
    try:
        if op:
            press('tab')
            sleep(INTERVALO_SLOW)
            press('tab')
            sleep(INTERVALO_SLOW)
        for y in range(3):
            press('down')
            sleep(INTERVALO_SLOW)
        press('enter')
        sleep(INTERVALO_SLOW)

        print("Parei...")

    except Exception as e:
        print(f"Erro ao tentar abrir o CAD: {e}")
    
def to_seach():
    for i in range(4):
        press('tab')
        sleep(0.7)
        # inicio de seleções

def recua_ate_filtro_pass(lc):
    # Recua até o filtro selecionado e passa para o proximo
    for i in range(4):
        hotkey('shift', 'tab')
        sleep(INTERVALO_SLOW)
    # Seleciona o proximo
    for k in range(lc):
        press('down')
        sleep(INTERVALO_SLOW)

    press('enter')
    for j in range(4):
        press('tab')
        sleep(INTERVALO_SLOW)


def interation_extract(lacos):
    # Faz a extração do 1º ao 6ºCOB e tambem CEB
    try:
        sleep(INTERVALO_SLOW_SUPER)
        press('enter')
        sleep(INTERVALO_SLOW_SUPER)
        hotkey('shift', 'tab')
        sleep(INTERVALO_SLOW_SUPER)
        press('enter')
        sleep(INTERVALO_SLOW_SUPER)
        check_and_kill_cad_jnlp("geany")
        sleep(INTERVALO_SLOW_SUPER)
        recua_ate_filtro_pass(lacos)

        
    except Exception as e:
        print(f"Erro ao tentar abrir o CAD: {e}")


def exportation_common():
    try:
        press('tab')
        sleep(INTERVALO_SLOW_SUPER)
        press('enter')
        sleep(INTERVALO_SLOW_SUPER)
        hotkey('shift', 'tab')
        sleep(INTERVALO_SLOW_SUPER)
        press('enter')
        sleep(INTERVALO_SLOW_SUPER)

    except Exception as e:
        print(f"Erro ao tentar abrir o CAD: {e}")


# Função que inicia o cad, insere os dados de login e fica pronto para realizar os filtros
def init_system():
    abrir_cad()
    login_cad()
    sleep(5)


def merge_csvs():
    # Caminho dos arquivos
    input_folder = "/tmp/cliente-cad"  # Substitua pelo caminho real
    output_file = "arquivo_mesclado.csv"
    
    # Faz um merge de todos os arquivos e deleta a pasta temporária
    merge_csv_files(input_folder, output_file)
    count_records("arquivo_mesclado.csv")
    delete_all_files(input_folder)


def verity_and_reinit_system():

    process_alive = None

    try:
        process_alive = veryfy_if_is_running('cad.jnlp')
        sleep(INTERVALO_SLOW)

    except Exception as e:
        process_alive = None
        print(f"Erro: {e}")

    if process_alive:
        print(f"conteudo de process_alive: {process_alive}")
        selecoes(False)
        sleep(INTERVALO_SLOW_SUPER)
        to_seach()
        sleep(INTERVALO_SLOW_SUPER)
        for n in range(1):
            interation_extract(2)
            sleep(INTERVALO_SLOW_SUPER)
        interation_extract(3)
        sleep(INTERVALO_SLOW_SUPER)
        merge_csvs()
        sleep(INTERVALO_SLOW_SUPER)
        sync_csv_to_database()
        sleep(INTERVALO_SLOW_SUPER)
        reset_fields()
        return

    else:
        try:
            check_and_kill_cad_jnlp("cad.jnlp")
            print("O cad não está aberto na maquina")
            sleep(2)
            init_system()
            sleep(5)
            print("Verificando seleçoes")
            selecoes(True)
            sleep(INTERVALO_SLOW_SUPER)
            to_seach()
            sleep(INTERVALO_SLOW_SUPER)
            for n in range(5):
                interation_extract(2)
                sleep(INTERVALO_SLOW_SUPER)
            interation_extract(3)
            sleep(INTERVALO_SLOW_SUPER)
            merge_csvs()
            sleep(INTERVALO_SLOW_SUPER)
            sync_csv_to_database()
            sleep(INTERVALO_SLOW_SUPER)
            reset_fields()
        except Exception as e:
            print(f"Erro: {e}")
            check_and_kill_cad_jnlp("cad.jnlp")

if __name__ == "__main__":
    # Chama a função para abrir o CAD
    while True:
        verity_and_reinit_system()
        sleep(40)

