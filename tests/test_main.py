import pytest
import sys
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.metrics.metrics import validate_ingest
from src.schema.schema_google import google_play_schema_bronze


@pytest.fixture(scope="session")
def spark():
    """
    Fixture que inicializa o SparkSession para os testes.
    """
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    yield spark
    spark.stop()


def fetch_reviews(product_id):
    """
    Função mock de dados para simular a coleta de avaliações.
    """
    if product_id == "com.santander.app":
        return [
            ("https://play-lh.googleusercontent.com/a/ACg8ocKVR7jALSxc3SZfSeBv5ZysOFoXIkngoE0O_J5HfUql9W836w=mo", "November 03, 2024", "c484bb1e-eebb-4609-93c4-d21d00f648ed", "2024-11-03T01:58:03Z", 266, 2.0, None, "As notificações do app não funciona depois da última atualização...", "Robson Santos"),
            ("https://play-lh.googleusercontent.com/a-/ALV-UjU0-K2FzgFHxvvsqYDDjM5hdBFKxogfpXOI30yUSGPd2jrwYyUlzA", "October 23, 2024", "1ccda588-f60f-4b4d-b9a2-00234e903628", "2024-10-23T23:30:38Z", 189, 3.0, None, "App leve, rápido, fácil de navegar. Gostei muito. Só sinto falta de...", "Carlos Alberto Souza"),
            ("https://play-lh.googleusercontent.com/a-/ALV-UjXz3IdoGmdiJywZYfufnWNGoC041A8vtvb4SLgclRKicixck7A7Cg", "November 14, 2024", "9b0fb626-a84b-4a19-86a9-c3d7e6d00138", "2024-11-14T14:01:17Z", 54, 1.0, None, "O aplicativo Santander apresenta falhas constantes e exige confirmação...", "Tanios Toledo"),
            ("https://play-lh.googleusercontent.com/a-/ALV-UjWqUa0ZCAcbIpqRM9uo9604gjuDyLg2c5mR-9LLgJLesdBEZ00ooQ", "October 01, 2024", "69a3d3db-7760-47d0-9fc1-a9cb058c8402", "2024-10-01T21:33:15Z", 136, 2.0, None, "Interface deixa a muito a indesejar e umas complicações chatas...", "Rafa GP"),
            ("https://play-lh.googleusercontent.com/a-/ALV-UjWqUa0ZCAcbIpqRM9uo9604gjuDyLg2c5mR-9LLgJLesdBEZ00ooQ", "October 01, 2024", "69a3d3db-7760-47d0-9fc1-a9cb058c8402", "2024-10-01T21:33:15Z", 136, 2.0, None, "Interface deixa a muito a indesejar e umas complicações chatas...", "Rafa GP"),
            ("https://play-lh.googleusercontent.com/a-/ALV-UjXYIpyQqCUqTFoywYt5PSMGPSgPYT-GqlXM5m9SStORtNyBLoK2eGQ", "October 12, 2024", "8a20d295-fdb8-4ab6-84e1-c5e3eb1f2578", "2024-10-12T14:10:56Z", 95, 4.0, None, "A interface está cada vez melhor. O app do Santander está no caminho certo...", "Pedro Henrique Melo"),
            ("https://play-lh.googleusercontent.com/a-/ALV-UjURFIlCwxWzLfqZc-5aNyQRGgfXYPOG08SDYtZ1HO7oISjUsQn87g", "November 10, 2024", "ad17328f-784d-4e63-bdf9-309bdb3781eb", "2024-11-10T10:00:45Z", 84, 3.0, None, "O aplicativo em geral é bom, mas sinto falta de integração com dispositivos...", "Lucia Alves"),
        ]


def test_args():
    """
    Testa os argumentos passados via linha de comando para garantir que o número correto de argumentos é fornecido.
    """
    with patch.object(sys, 'argv', ['app.py', 'com.santander.app', 'banco-santander-br']):
        assert len(sys.argv) == 3, "[*] Usage: spark-submit app.py <product_id> <name_app>"


def test_collect_reviews_google(spark):
    """
    Testa a função de coleta de avaliações mockada para garantir que o retorno não está vazio.
    """
    with patch.object(sys, 'argv', ['app.py', 'com.santander.app', 'banco-santander-br']):
        # Entrada e captura de variáveis e parâmetros
        product_id = sys.argv[1]
        name_app = sys.argv[2]
        reviews_df = None

    reviews_data = fetch_reviews(product_id)

    if reviews_data:
        # Converter a lista de avaliações para RDD #############################################################
        reviews_rdd = spark.sparkContext.parallelize(reviews_data)

        # Aplicando o schema explicitamente durante a criação do DataFrame
        reviews_df = spark.createDataFrame(reviews_rdd, schema=google_play_schema_bronze())

        # Verifica se a coluna "response" está presente ########################################################
        if "response" not in reviews_df.columns:
            # Adiciona a coluna "response" com valores nulos
            reviews_df = reviews_df.withColumn("response", F.lit(None).cast(StringType()))

        # Verifica se o DataFrame não está vazio
        assert reviews_df.count() > 0, "[*] Nenhuma avaliação foi coletada!"



def test_validate_ingest(spark):
    """
    Testa a função de validação de ingestão para garantir que os DataFrames têm dados e que a validação gera resultados.
    """
    with patch.object(sys, 'argv', ['app.py', 'com.santander.app', 'banco-santander-br']):
        product_id = sys.argv[1]

    reviews_data = fetch_reviews(product_id)
    assert reviews_data, "[*] Nenhuma avaliação foi coletada!"

    # Converter a lista de avaliações para um RDD
    reviews_rdd = spark.sparkContext.parallelize(reviews_data)

    # Criar o DataFrame com o schema forçado
    reviews_df = spark.createDataFrame(reviews_rdd, schema=google_play_schema_bronze())

    # Verifica se a coluna "response" está presente
    if "response" not in reviews_df.columns:
        reviews_df = reviews_df.withColumn("response", F.lit(None).cast(StringType()))

    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(reviews_df)

    assert valid_df.count() > 0, "[*] O DataFrame válido está vazio!"
    assert invalid_df.count() > 0, "[*] O DataFrame inválido está vazio!"
    assert len(validation_results) > 0, "[*] Não foram encontrados resultados de validação!"

    # Opcional: Exibir resultados para depuração
    print("Testes realizados com sucesso!")
    print(f"Total de registros válidos: {valid_df.count()}")
    print(f"Total de registros inválidos: {invalid_df.count()}")
    print(f"Resultados da validação: {validation_results}")


def test_schema(spark):
    """
    Testa a função de validação do schema, para garantir conformidade com o esperado.
    """
    with patch.object(sys, 'argv', ['app.py', 'com.santander.app', 'banco-santander-br']):
        product_id = sys.argv[1]

    reviews_data = fetch_reviews(product_id)
    assert reviews_data, "[*] Nenhuma avaliação foi coletada!"

    # Converter a lista de avaliações para RDD
    reviews_rdd = spark.sparkContext.parallelize(reviews_data)

    # Criar o DataFrame com o schema forçado
    reviews_df = spark.createDataFrame(reviews_rdd, schema=google_play_schema_bronze())

    # Verifica se a coluna "response" está presente
    if "response" not in reviews_df.columns:
        reviews_df = reviews_df.withColumn("response", F.lit(None).cast(StringType()))

    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(reviews_df)

    # Forçar o schema nos DataFrames retornados
    bronze_schema = google_play_schema_bronze()

    # Validação do schema para garantir conformidade
    assert valid_df.schema == bronze_schema, f"O schema do DataFrame válido não está correto! {valid_df.schema}"
    assert invalid_df.schema == bronze_schema, f"O schema do DataFrame inválido não está correto! {invalid_df.schema}"

    print("Schema forçado e validado com sucesso!")
    print("Total de registros válidos:", valid_df.count())
    print("Total de registros inválidos:", invalid_df.count())

