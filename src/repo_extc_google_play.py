import sys
import os
import json
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, date_format
from pyspark.sql.types import StringType
from elasticsearch import Elasticsearch
from metrics import MetricsCollector, validate_ingest
from tools import fetch_reviews, save_dataframe, save_metrics

from src.schema.schema_google import google_play_schema_bronze

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constantes da Aplicação ---
NUM_PAGES_DEFAULT = 10
PATH_TARGET_BASE = "/santander/bronze/compass/reviews/googlePlay/"
PATH_TARGET_FAIL_BASE = "/santander/bronze/compass/reviews_fail/googlePlay/"
ENV_PRE_VALUE = "pre"
ELASTIC_INDEX_SUCCESS = "compass_dt_datametrics"
ELASTIC_INDEX_FAIL = "compass_dt_datametrics_fail"


def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(description="Processa avaliações da Apple Store.")
    parser.add_argument("<env>", type=str, help="Ambiente de execução (ex: 'pre', 'prod').")
    parser.add_argument("<product_id>", type=str, help="ID da avaliação do aplicativo na Loja do Google Play.")
    parser.add_argument("<name_app>", type=str, help="Nome do aplicativo.")
    parser.add_argument("<type_client>", type=str, help="Tipo de cliente.")
    return parser.parse_args()

def main():
    """
    Capturar argumentos da linha de comando usando argparse
    args = parse_arguments() # Descomente esta linha e comente a de baixo para usar argparse
    No entanto, para manter a compatibilidade com a estrutura original de sys.argv,
    continuaremos usando sys.argv, mas a recomendação é usar argparse.
    """
    args = sys.argv

    # Verificar se o número correto de argumentos foi passado
    if len(args) != 5:
        logging.error(f"[*] Usage: spark-submit app.py <product_id> <name_app>")
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
        # Coleta de métricas 
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()

        # Buscar avaliações ######
        reviews_data = fetch_reviews(product_id)

        if reviews_data:
            # Converter a lista de avaliações para RDD #############################################################
            reviews_rdd = spark.sparkContext.parallelize(reviews_data)
            reviews_df = spark.createDataFrame(reviews_rdd)

            # Verifica se a coluna "response" está presente 
            if "response" not in reviews_df.columns:
                # Adiciona a coluna "response" com valores nulos
                reviews_df = reviews_df.withColumn("response", F.lit(None).cast(StringType()))

            # Definicao dos paths 
            path_target = f"{PATH_TARGET_BASE}{name_app}_{type_client}/"
            path_target_fail = f"{PATH_TARGET_FAIL_BASE}{name_app}_{type_client}/"

            # Valida o DataFrame e coleta resultados
            valid_df, invalid_df, validation_results = validate_ingest(reviews_df)

            if env == ENV_PRE_VALUE: # Usando constante
                valid_df.take(10)

            # Salvar dados válidos
            save_dataframe(
                df=valid_df,
                path=path_target,
                label="valido",
                schema=google_play_schema_bronze(),
                partition_column="odate", # data de carga referencia
                compression="snappy"
            )

            # Salvar dados inválidos
            save_dataframe(
                df=invalid_df,
                path=path_target_fail,
                label="invalido",
                partition_column="odate", # data de carga referencia
                compression="snappy"
            )

            # Finaliza coleta de metricas
            metrics_collector.end_collection()

            # Coleta métricas após o processamento
            metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, name_app, type_client)

            # Salvar métricas
            save_metrics(metrics_json)
            logging.info(f"[*] Métricas da aplicação: {json.loads(metrics_json)}")
        else:
            logging.error("[*] Nenhuma avaliação encontrada para o product_id fornecido.")

    except Exception as e:
        logging.error(f"[*] Erro ao processar avaliações: {e}", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e,
            client="UNKNOWN_CLIENT"
        )
    finally:
        spark.stop()



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
