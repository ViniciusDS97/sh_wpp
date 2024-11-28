# @title Baixando dependências e configurações

from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import http.client
import requests
import hashlib
import aiohttp
import asyncio
import base64
import uuid
import json
import re
import os
from io import BytesIO 
from PyInstaller.utils.hooks import collect_submodules
hiddenimports = collect_submodules('asyncio')

# @title Configurações/Variáveis/autenticações

# Configurando a forma de visualização
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

mapa_meses = {
    'Jan': 'Jan', 'Feb': 'Fev', 'Mar': 'Mar', 'Apr': 'Abr',
    'May': 'Mai', 'Jun': 'Jun', 'Jul': 'Jul', 'Aug': 'Ago',
    'Sep': 'Set', 'Oct': 'Out', 'Nov': 'Nov', 'Dec': 'Dez'
}

# Carregando credenciais de um arquivo JSON local
with open('Z:\\Documentados\\API - Mensagens WPP\\credentials.json', 'r') as f:
    credenciais = json.load(f)

credenciaisAPI = credenciais['API_DRIVE']
data_base_excel = pd.Timestamp('1899-12-30')
LOGIN_OA_COMMA = credenciais['API_DRIVE']['API_COMMA_LOGIN']
SECRET_OA_COMMA = credenciais['API_DRIVE']['API_OA_COMMA_SECRET']
numero_whatsapp_oriba = credenciais['5511915654337']

credenciais_dict = credenciaisAPI

# Definindo escopo de autorizações
SCOPES = ['https://www.googleapis.com/auth/drive']

# Compilando credenciais de usuário
creds = service_account.Credentials.from_service_account_info(credenciais_dict, scopes=SCOPES)

# Construindo credencial do cliente
service = build('drive', 'v3', credentials=creds)

# @title Funções

# Função para localizar arquivo no driver
folder_id = '1E1di17E44TaAokm-aoYOKUDHQYpRIIqM'

def list_files_in_folder(folder_id):
    query = f"'{folder_id}' in parents"
    results = service.files().list(q=query,
                                   spaces='drive',
                                   fields='nextPageToken, files(id, name, mimeType)').execute()
    return results.get('files', [])

def mover_arquivo_para_nova_pasta(file_id, nova_pasta_id):
    # Obtém as permissões do arquivo atual para removê-las da pasta original
    file = service.files().get(fileId=file_id, fields='parents').execute()
    previous_parents = ",".join(file.get('parents'))

    # Move o arquivo para a nova pasta
    file = service.files().update(
        fileId=file_id,
        addParents=nova_pasta_id,
        removeParents=previous_parents,
        fields='id, parents'
    ).execute()
    
    print(f"Arquivo {file_id} movido para a pasta {nova_pasta_id}")

def download_and_merge_files(folder_id):
    files = list_files_in_folder(folder_id)
    combined_df = pd.DataFrame()

    for file in files:
        file_id = file['id']
        filename = file['name']

        print(f"Baixando e processando arquivo: {filename}")

        try:
            request = service.files().get_media(fileId=file_id)
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}% concluído.")

            fh.seek(0)
            if filename.endswith('.csv'):
                temp_df = pd.read_csv(fh, sep=';', quotechar='"', on_bad_lines='skip', low_memory=False)
            elif filename.endswith('.xlsx') or filename.endswith('.xls'):
                temp_df = pd.read_excel(fh)
            else:
                print(f"Formato não suportado: {filename}. Pulando arquivo.")
                continue

            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

            # Mover o arquivo para a nova pasta após o processamento
            mover_arquivo_para_nova_pasta(file_id, '1_a_zWhshuwZ34FUvdos-BTqRK8nNjr85')

        except Exception as e:
            print(f"Erro ao processar o arquivo '{filename}': {e}")

    return combined_df

df_arquivos_disparo = download_and_merge_files(folder_id)

# Autenticação Comma API
auth_string = f"{LOGIN_OA_COMMA}:{SECRET_OA_COMMA}"
auth_encoded = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

headers = {
    'Authorization': f'Basic {auth_encoded}',
    'Content-Type': 'application/json'
}

df_arquivos_disparo_pipe = df_arquivos_disparo.copy()
df_arquivos_disparo_pipe = df_arquivos_disparo_pipe['telefone,urldinamic'].str.split(',', expand=True)
df_arquivos_disparo_pipe = df_arquivos_disparo_pipe.rename(columns={0: "telefone", 1: "urldinamic"})

# API Endpoint
url_api = "https://comma-backend.azurewebsites.net/api/v1/send/whatsapp/template"

lote_tamanho = 1
lotes = np.array_split(df_arquivos_disparo_pipe, len(df_arquivos_disparo_pipe) // lote_tamanho)

resultados = []

async def enviar_mensagem(row):
    telefone = row['telefone']
    urldinamic = row['urldinamic']
    payload = {
        "actionType": "teste_disparo_sh_cx_nps_20241123",
        "appName": "SHOUDERCXLOJA",
        "email": "giulia.canedo@shoulder.com.br",
        "phone": '55' + telefone,
        "showTemplateContent": False,
        "template": {
            "category": "MARKETING",
            "name": "nps_csat_loja_fisica",
            "params": [urldinamic]
        },
        "triggerName": "teste_disparo_sh_cx_nps_20241123"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url_api, headers=headers, json=payload) as response:
                if response.status == 200:
                    print(f"Mensagem enviada para {telefone} com sucesso!")
                    resultados.append({"telefone": telefone, "status": "Sucesso", "erro": None})
                else:
                    print(f"Erro ao enviar mensagem para {telefone}: {response.status} - {await response.text()}")
                    resultados.append({"telefone": telefone, "status": "Erro", "erro": f"{response.status} - {await response.text()}"})
        except Exception as e:
            print(f"Erro inesperado para {telefone}: {e}")
            resultados.append({"telefone": telefone, "status": "Erro", "erro": str(e)})

async def processar_lote(lote):
    tasks = []
    for _, row in lote.iterrows():
        task = enviar_mensagem(row)
        tasks.append(task)
    await asyncio.gather(*tasks)

# Função principal
async def main():
    # Envia as mensagens em lotes
    for lote in lotes:
        await processar_lote(lote)

    # Salva os resultados
    df_resultados = pd.DataFrame(resultados)

    # Exporta os resultados para um arquivo CSV e Excel
    data_hora_atual = datetime.now().strftime("%Y%m%d-%H%M%S")
    nome_arquivo = f"Verificação_{data_hora_atual}.xlsx"
    df_resultados.to_csv("resultados_envio.csv", index=False)
    df_resultados.to_excel(nome_arquivo, index=False)

    print(f"Arquivo {nome_arquivo} gerado com sucesso!")

# Substituindo await main() por:
if __name__ == '__main__':
    asyncio.run(main())