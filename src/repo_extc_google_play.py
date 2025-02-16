import logging
import sys
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, date_format
from pyspark.sql.types import StringType
from datetime import datetime
from metrics import MetricsCollector, validate_ingest
from tools import *
from elasticsearch import Elasticsearch


def main():
    # Capturar argumentos da linha de comando
    args = sys.argv

    print("[*] ARGUMENTOS: " + str(args))

    # Verificar se o número correto de argumentos foi passado
    if len(args) != 5:
        print("[*] Usage: spark-submit app.py <product_id> <name_app>")
        sys.exit(1)

    # Entrada e captura de variaveis e parametros
    env = args[1]
    product_id = args[2]
    name_app = args[3]
    type_client = args[4]
    reviews_df = None


    # Criação da sessão Spark
    spark = spark_session()

    try:
        # Coleta de métricas #######################################################################################
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()

        # Buscar avaliações ########################################################################################
        reviews_data = fetch_reviews(product_id)

        if reviews_data:
            # Converter a lista de avaliações para RDD #############################################################
            reviews_rdd = spark.sparkContext.parallelize(reviews_data)
            reviews_df = spark.createDataFrame(reviews_rdd)

            # Verifica se a coluna "response" está presente ########################################################
            if "response" not in reviews_df.columns:
                # Adiciona a coluna "response" com valores nulos
                reviews_df = reviews_df.withColumn("response", F.lit(None).cast(StringType()))

            # Definicao dos paths ##################################################################################
            datePath = datetime.now()
            path_target = f"/santander/bronze/compass/reviews/googlePlay/{name_app}_{type_client}/odate={datePath.strftime('%Y%m%d')}/"
            path_target_fail = f"/santander/bronze/compass/reviews_fail/googlePlay/{name_app}_{type_client}/odate={datePath.strftime('%Y%m%d')}/"

            # Valida o DataFrame e coleta resultados
            valid_df, invalid_df, validation_results = validate_ingest(reviews_df)

            # Salvar dados válidos
            save_dataframe(valid_df, path_target, "valido")

            # Salvar dados inválidos
            save_dataframe(invalid_df, path_target_fail, "invalido")

            # Finaliza coleta de metricas
            metrics_collector.end_collection()

            # Coleta métricas após o processamento
            metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, name_app, type_client)

            # Salvar métricas
            save_metrics(metrics_json)

            print(f"[*] Métricas da aplicação: {json.loads(metrics_json)}")
        else:
            print("[*] Nenhuma avaliação encontrada para o product_id fornecido.")

    except Exception as e:
        print(f"[*] Erro ao criar o DataFrame: {e}")
        logging.error(f"[*] Erro ao processar avaliações: {e}", exc_info=True)
        send_metrics_fail(e)


    finally:
        spark.stop()

def send_metrics_fail(e):

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics_fail"
    ES_USER = os.environ["ES_USER"]
    ES_PASS = os.environ["ES_PASS"]

    # Conectar ao Elasticsearch
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASS)
    )

    # JSON de erro
    error_metrics = {
        "timestamp": date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        "camada": "bronze",
        "grupo": "compass",
        "job": "google_play_reviews",
        "relevancia": "0",
        "torre": "SBBR_COMPASS",
        "client": type_client,
        "erro": str(e)
    }

    metrics_json = json.dumps(error_metrics)

    try:
        # Converter JSON em dicionário
        metrics_data = json.loads(metrics_json)

        # Inserir no Elasticsearch
        response = es.index(index=ES_INDEX, document=metrics_data)

        logging.info(f"[*] Métricas da aplicação salvas no Elasticsearch: {response}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"[*] Erro ao salvar métricas no Elasticsearch: {e}", exc_info=True)

def save_metrics(metrics_json):
    """
    Salva as métricas.
    """

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics"
    ES_USER = os.environ["ES_USER"]
    ES_PASS = os.environ["ES_PASS"]

    # Conectar ao Elasticsearch
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASS)
    )

    try:
        # Converter JSON em dicionário
        metrics_data = json.loads(metrics_json)

        # Inserir no Elasticsearch
        response = es.index(index=ES_INDEX, document=metrics_data)

        logging.info(f"[*] Métricas da aplicação salvas no Elasticsearch: {response}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"[*] Erro ao salvar métricas no Elasticsearch: {e}", exc_info=True)




def spark_session():
    try:
        spark = SparkSession.builder \
            .appName("App Reviews origem [google play]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .getOrCreate()
        return spark

    except Exception as e:
        logging.error(f"[*] Falha ao criar SparkSession: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
