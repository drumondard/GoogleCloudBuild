import os
import re
from google.cloud import storage, bigquery
import pandas as pd
from io import BytesIO
from datetime import datetime
import sys
import io

# Data atual para formar prefixo e nome tabela, etc
AGORA = datetime.now()
ANO = AGORA.strftime("%Y")
ANO_MES = AGORA.strftime("%Y%m")

# Caminho para a chave de serviço
key_path = "C:/Users/vt322614/OneDrive/Python/ContaSevico/vtal-sandbox-engenharia-bfe3a49a6e20.json"

# Autenticação
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Configurações
BUCKET_NAME = "vtal-land-zone-engenharia"
PREFIX = f"Inventario/sci_comunicados/abertos/{ANO}/{ANO_MES}/"
BQ_PROJECT = "vtal-sandbox-engenharia"
BQ_DATASET = "inventario_comunicados_interrupcao"
BQ_TABLE = f"tb_comunicados_ab_foto_dia"
 
# Clientes
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blobs = list(storage_client.list_blobs(bucket, prefix=PREFIX))

# Regex para extrair data e hora do nome do arquivo
regex = re.compile(r".*_(\d{2})(\d{2})(\d{4})_(\d{2})_(\d{2})_(\d{2})\.csv$")

# Mapear menor horário por data
arquivos_por_dia = {}

for blob in blobs:
    nome_arquivo = blob.name.split("/")[-1]
    if not nome_arquivo.endswith(".csv"):
        continue
    match = regex.match(nome_arquivo)
    if not match:
        continue

    dia, mes, ano, hora, minuto, segundo = match.groups()
    data_str = f"{ano}-{mes}-{dia}"
    hora_str = f"{hora}:{minuto}:{segundo}"
    data_hora = datetime.strptime(f"{data_str} {hora_str}", "%Y-%m-%d %H:%M:%S")

    if data_str not in arquivos_por_dia or data_hora < arquivos_por_dia[data_str]["data_hora"]:
        arquivos_por_dia[data_str] = {
            "blob": blob,
            "data_hora": data_hora,
            "nome_arquivo": nome_arquivo,
        }

# Carregar dados e unir em um DataFrame
dataframes = []
arquivos_ignorados = []

for data_str, info in sorted(arquivos_por_dia.items()):
    blob = info["blob"]
    try:
        content = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(content), sep=";", encoding="utf-8", low_memory=False)
        df["data_arquivo"] = data_str
        df["hora_arquivo"] = info["data_hora"].strftime("%H:%M:%S")
        df["nome_arquivo"] = info["nome_arquivo"]
        dataframes.append(df)
    except Exception as e:
        arquivos_ignorados.append((info["nome_arquivo"], str(e)))

if dataframes:
    df_final = pd.concat(dataframes, ignore_index=True)

    # Enviar para o BigQuery (modo append)
    bq_client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    job = bq_client.load_table_from_dataframe(df_final, table_id, job_config=job_config)
    job.result()

    print(f"[OK] {len(df_final)} registros adicionados na tabela '{table_id}'.")
else:
    print("[AVISO] Nenhum arquivo válido encontrado para processar.")

# Exibir arquivos ignorados (se houver)
if arquivos_ignorados:
    print("\n[!] Arquivos ignorados por erro de leitura:")
    for nome, erro in arquivos_ignorados:
        print(f" - {nome}: {erro}")
