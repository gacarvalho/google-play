import os
import json
import logging
import requests
import pymongo
from urllib.parse import quote_plus
from pyspark.sql import DataFrame
from pathlib import Path
from pyspark.sql.types import StringType, IntegerType
from schema_google import google_play_schema_bronze
from elasticsearch import Elasticsearch

def get_api_key():
    """Retorna a chave da API armazenada nas variáveis de ambiente."""
    api_key = os.environ["SERPAPI_KEY"]

    if not api_key:
        raise ValueError("[*] Chave da API SerpApi não encontrada. Verifique o arquivo .env")
    return api_key

def fetch_reviews(product_id, reviews_per_page=199, max_pages=10):
    """
    Busca avaliações de um produto da Google Play Store usando SerpApi diretamente com requests.

    Args:
        product_id (str): O ID do produto na Google Play Store.
        reviews_per_page (int): Número de avaliações por página (default: 199).
        max_pages (int): Número máximo de páginas a buscar (default: 10).

    Returns:
        list: Lista de avaliações.
    """
    api_key = get_api_key()
    all_reviews = []
    next_page_token = None
    page_count = 0

    while page_count < max_pages:
        params = {
            "api_key": api_key,
            "engine": "google_play_product",
            "product_id": product_id,
            "store": "apps",
            "all_reviews": "true",
            "gl": "br",
            "hl": "pt-BR",
            "num": reviews_per_page
        }

        if next_page_token:
            params["next_page_token"] = next_page_token

        try:
            response = requests.get("https://serpapi.com/search", params=params)
            response.raise_for_status()
            results = response.json()

            if 'reviews' not in results:
                raise Exception("[*] Não foi possível recuperar as avaliações.")

            reviews = results['reviews']
            all_reviews.extend(reviews)

            next_page_token = results.get('serpapi_pagination', {}).get('next_page_token')

            if not next_page_token:
                break

            page_count += 1

        except requests.exceptions.RequestException as e:
            print(f"[*] Erro ao fazer a requisição: {e}")
            send_metrics_fail(e)
            break

    return all_reviews

def save_reviews(reviews_df: DataFrame, directory: str):
    """
    Salva os dados do DataFrame no formato parquet no diretório especificado.

    Args:
        reviews_df (DataFrame): DataFrame PySpark contendo as avaliações.
        directory (str): Caminho do diretório onde os dados serão salvos.
    """
    try:
        # Verifica se o diretório existe e cria-o se não existir
        Path(directory).mkdir(parents=True, exist_ok=True)

        reviews_df.write.option("compression", "snappy").mode("overwrite").parquet(directory)
        logging.info(f"[*] Dados salvos em {directory} no formato parquet")

    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}")



def write_to_mongo(dados_feedback: dict, table_id: str):

    mongo_user = os.environ["MONGO_USER"]
    mongo_pass = os.environ["MONGO_PASS"]
    mongo_host = os.environ["MONGO_HOST"]
    mongo_port = os.environ["MONGO_PORT"]
    mongo_db = os.environ["MONGO_DB"]

    # ---------------------------------------------- Escapar nome de usuário e senha ----------------------------------------------
    # A função quote_plus transforma caracteres especiais em seu equivalente escapado, de modo que o
    # URI seja aceito pelo MongoDB. Por exemplo, m@ngo será convertido para m%40ngo.
    escaped_user = quote_plus(mongo_user)
    escaped_pass = quote_plus(mongo_pass)

    # ---------------------------------------------- Conexão com MongoDB ----------------------------------------------------------
    # Quando definimos maxPoolSize=1, estamos dizendo ao MongoDB para manter apenas uma conexão aberta no pool.
    # Isso implica que cada vez que uma nova operação precisa de uma conexão, a conexão existente será
    # reutilizada em vez de criar uma nova.
    mongo_uri = f"mongodb://{escaped_user}:{escaped_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource={mongo_db}&maxPoolSize=1"

    client = pymongo.MongoClient(mongo_uri)

    try:
        db = client[mongo_db]
        collection = db[table_id]

        # Inserir dados no MongoDB
        if isinstance(dados_feedback, dict):  # Verifica se os dados são um dicionário
            collection.insert_one(dados_feedback)
        elif isinstance(dados_feedback, list):  # Verifica se os dados são uma lista
            collection.insert_many(dados_feedback)
        else:
            print("[*] Os dados devem ser um dicionário ou uma lista de dicionários.")
    finally:
        # Garante que a conexão será fechada
        client.close()

def get_schema(df, schema):
    """
    Obtém o DataFrame a seguir o schema especificado.
    """
    for field in schema.fields:
        if field.dataType == IntegerType():
            df = df.withColumn(field.name, df[field.name].cast(IntegerType()))
        elif field.dataType == StringType():
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    return df.select([field.name for field in schema.fields])

def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:
        schema = google_play_schema_bronze()
        # Alinhar o DataFrame ao schema definido
        df = get_schema(df, schema)

        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"[*] Salvando dados {label} para: {path}")
            save_reviews(df, path)
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)



