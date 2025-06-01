import os
import json
import logging
import requests
import traceback
from pathlib import Path
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import current_timestamp, date_format, lit
from elasticsearch import Elasticsearch
from urllib.parse import quote_plus
from typing import Optional, Union
from schema_google import google_play_schema_bronze


# --- Constantes da Aplicação ---
ELASTIC_INDEX_SUCCESS = "compass_dt_datametrics"
ELASTIC_INDEX_FAIL = "compass_dt_datametrics_fail"

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
            logging.error(f"[*] Erro ao fazer a requisição: {e}")
            save_metrics(
                metrics_type="fail",
                index=ELASTIC_INDEX_FAIL,
                error=e,
                client="UNKNOWN_CLIENT"
            )
            break

    return all_reviews



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

def save_dataframe(df: DataFrame,
                   path: str,
                   label: str,
                   schema: Optional[dict] = None,
                   partition_column: str = "odate",
                   compression: str = "snappy") -> bool:
    """
    Salva um DataFrame Spark no formato Parquet de forma robusta e profissional.

    Parâmetros:
        df: DataFrame Spark a ser salvo
        path: Caminho de destino para salvar os dados
        label: Identificação do tipo de dados para logs (ex: 'valido', 'invalido')
        schema: Schema opcional para validação dos dados
        partition_column: Nome da coluna de partição (default: 'odate')
        compression: Tipo de compressão (default: 'snappy')

    Retorno:
        bool: True se salvou com sucesso, False caso contrário

    Exceções:
        ValueError: Se os parâmetros obrigatórios forem inválidos
        IOError: Se houver problemas ao escrever no filesystem
    """

    # Validação dos parâmetros
    if not isinstance(df, DataFrame):
        logging.error(f"Objeto passado não é um DataFrame Spark: {type(df)}")
        return False

    if not path:
        logging.error("Caminho de destino não pode ser vazio")
        return False

    # Configuração
    current_date = datetime.now().strftime('%Y%m%d')
    full_path = Path(path)

    try:
        # Aplicar schema se fornecido
        if schema:
            logging.info(f"[*] Aplicando schema para dados {label}")
            df = get_schema(df, schema)


        # Adicionar coluna de partição
        df_partition = df.withColumn(partition_column, lit(current_date))

        # Verificar se há dados
        if not df_partition.head(1):
            logging.error(f"[*] Nenhum dado {label} encontrado para salvar")
            return False

        # Preparar diretório
        try:
            full_path.mkdir(parents=True, exist_ok=True)
            logging.debug(f"[*] Diretório {full_path} verificado/criado")

        except Exception as dir_error:
            logging.error(f"[*] Falha ao preparar diretório {full_path}: {dir_error}")
            raise IOError(f"[*] Erro de diretório: {dir_error}") from dir_error

        # Escrever dados
        logging.info(f"[*] Salvando {df_partition.count()} registros ({label}) em {full_path}")

        (df_partition.write
         .option("compression", compression)
         .mode("overwrite")
         .partitionBy(partition_column)
         .parquet(str(full_path)))

        logging.info(f"[*] Dados {label} salvos com sucesso em {full_path}")
        return True

    except Exception as e:
        error_msg = f"[*] Falha ao salvar dados {label} em {full_path}"
        logging.error(error_msg, exc_info=True)
        logging.error(f"[*] Detalhes do erro: {str(e)}\n{traceback.format_exc()}")
        return False

def save_metrics(metrics_type: str,
                 index: str,
                 error: Optional[Exception] = None,
                 client: Optional[str] = None,
                 metrics_data: Optional[Union[dict, str]] = None) -> None:
    """
    Salva métricas no Elasticsearch mantendo estruturas específicas para cada tipo.

    Parâmetros:
        metrics_type: 'success' ou 'fail' (case insensitive)
        index: Nome do índice no Elasticsearch
        error: Objeto de exceção (obrigatório para tipo 'fail')
        client: Nome do cliente (opcional)
        metrics_data: Dados das métricas (obrigatório para 'success', ignorado para 'fail')

    Estruturas:
        - FAIL: Mantém estrutura fixa com informações de erro
        - SUCCESS: Mantém exatamente o que foi passado em metrics_data
    """
    # Converter para minúsculas para padronização
    metrics_type = metrics_type.lower()

    # Validações iniciais
    if metrics_type not in ('success', 'fail'):
        raise ValueError("[*] O tipo deve ser 'success' ou 'fail'")

    if metrics_type == 'fail' and not error:
        raise ValueError("[*] Para tipo 'fail', o parâmetro 'error' é obrigatório")

    if metrics_type == 'success' and not metrics_data:
        raise ValueError("[*] Para tipo 'success', 'metrics_data' é obrigatório")

    # Configuração do Elasticsearch
    ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
    ES_USER = os.getenv("ES_USER")
    ES_PASS = os.getenv("ES_PASS")

    if not all([ES_USER, ES_PASS]):
        raise ValueError("[*] Credenciais do Elasticsearch não configuradas")

    # Preparar o documento conforme o tipo
    if metrics_type == 'fail':
        document = {
            "timestamp": date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            "layer": "bronze",
            "project": "compass",
            "job": "google_play_reviews",
            "priority": "0",
            "torre": "SBBR_COMPASS",
            "client": client.upper() if client else "UNKNOWN_CLIENT",
            "error": str(error)
        }
    else:
        # Para success, usar exatamente o metrics_data fornecido
        if isinstance(metrics_data, str):
            try:
                document = json.loads(metrics_data)
            except json.JSONDecodeError as e:
                raise ValueError("[*] metrics_data não é um JSON válido") from e
        else:
            document = metrics_data

    # Conexão e envio para Elasticsearch
    try:
        es = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            request_timeout=30
        )

        response = es.index(
            index=index,
            document=document
        )

        logging.info(f"[*] Métricas salvas com sucesso no índice {index}. ID: {response['_id']}")
        return response

    except ElasticsearchException as es_error:
        logging.error(f"[*] Falha ao salvar no Elasticsearch: {str(es_error)}")
        raise
    except Exception as e:
        logging.error(f"[*] Erro inesperado: {str(e)}")
        raise