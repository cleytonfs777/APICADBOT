import requests
import time
import random
import json
import os

# Nome do arquivo para armazenar endereços já consultados
CACHE_FILE = "cache_coordenadas.json"

# Carregar cache se existir
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        cache = json.load(f)
else:
    cache = {}

def salvar_cache():
    """Salva o cache no arquivo JSON."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4, ensure_ascii=False)

def obter_coordenadas(endereco):
    """Consulta o Nominatim para obter as coordenadas do endereço."""
    
    # Verifica se o endereço já está no cache
    if endereco in cache:
        print(f'Cache encontrado para "{endereco}".')
        return cache[endereco]

    url = 'https://nominatim.openstreetmap.org/search'
    parametros = {'q': endereco, 'format': 'json'}
    headers = {'User-Agent': 'MeuAppGeolocalizacao/1.0 (contato@email.com)'}  # Personalize com seu e-mail

    try:
        resposta = requests.get(url, params=parametros, headers=headers, timeout=10)
        resposta.raise_for_status()  # Levanta erro HTTP se houver
        
        dados = resposta.json()
        if dados:
            latitude = float(dados[0]['lat'])
            longitude = float(dados[0]['lon'])

            # Aviso se for uma rua sem número específico
            if dados[0].get('addresstype') == 'road':
                print(f'ATENÇÃO: Número do endereço não encontrado, retornando coordenadas aproximadas da rua.')

            # Salvar no cache antes de retornar
            cache[endereco] = (latitude, longitude)
            salvar_cache()
            
            return latitude, longitude
        else:
            raise ValueError('Endereço não encontrado.')

    except requests.exceptions.RequestException as e:
        raise ConnectionError(f'Erro na conexão com a API: {e}')

def temporizador_aleatorio():
    """Aguarda um tempo aleatório entre 1 e 3 minutos para evitar bloqueios."""
    intervalo = random.uniform(30, 60)
    print(f'Aguardando {int(intervalo)} segundos antes da próxima requisição...')
    time.sleep(intervalo)

