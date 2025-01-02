import logging
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime
from metrics import MetricsCollector, validate_ingest
from tools import *


def main():
    # Capturar argumentos da linha de comando
    args = sys.argv

    print("[*] ARGUMENTOS: " + str(args))

    # Verificar se o número correto de argumentos foi passado
    if len(args) != 3:
        print("[*] Usage: spark-submit app.py <product_id> <name_app>")
        sys.exit(1)

    # Entrada e captura de variaveis e parametros
    product_id = args[1]
    name_app = args[2]
    reviews_df = None 

    try:
        # Criação da sessão Spark
        with spark_session() as spark:

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
                path_target = f"/santander/bronze/compass/reviews/googlePlay/{name_app}/odate={datePath.strftime('%Y%m%d')}/"
                path_target_fail = f"/santander/bronze/compass/reviews_fail/googlePlay/{name_app}/odate={datePath.strftime('%Y%m%d')}/"

                # Valida o DataFrame e coleta resultados
                valid_df, invalid_df, validation_results = validate_ingest(reviews_df)

                # Salvar dados válidos
                save_dataframe(valid_df, path_target, "valido")

                # Salvar dados inválidos
                save_dataframe(invalid_df, path_target_fail, "invalido")

                # Finaliza coleta de metricas
                metrics_collector.end_collection()

                # Coleta métricas após o processamento
                metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, name_app)
                # Salvar métricas no MongoDB
                save_metrics(metrics_json)
                
                print(f"[*] Métricas da aplicação: {json.loads(metrics_json)}")
            else:
                print("[*] Nenhuma avaliação encontrada para o product_id fornecido.")

    except Exception as e:
        print(f"[*] Erro ao criar o DataFrame: {e}")
        logging.error(f"[*] Erro ao processar avaliações: {e}", exc_info=True)
        send_metrics_fail()


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
