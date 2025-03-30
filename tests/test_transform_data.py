from pyspark.sql import SparkSession
from scripts.transform_data import fetch_and_create_dataframe

def test_fetch_and_create_dataframe():
    spark = SparkSession.builder.appName("TestApp").getOrCreate()

    # Executa a função para criar os DataFrames
    fetch_and_create_dataframe()

    # Verificar se os DataFrames foram criados corretamente
    bronze_df = spark.read.json("gs://bees_case/bronze_breweries")
    assert bronze_df is not None
    assert bronze_df.count() > 0  # Verificar se há dados

    silver_df = spark.read.parquet("gs://bees_case/silver_breweries")
    assert silver_df is not None
    assert silver_df.count() > 0

    gold_df = spark.read.parquet("gs://bees_case/gold_breweries")
    assert gold_df is not None
    assert gold_df.count() > 0

    # Verificar se as colunas esperadas estão presentes
    assert "id" in bronze_df.columns
    assert "name" in silver_df.columns
    assert "count" in gold_df.columns

    # verificação de partição
    assert "state=" in silver_df.inputFiles()[0]
    assert "state=" in gold_df.inputFiles()[0]

    spark.stop()

# Executa o teste
test_fetch_and_create_dataframe()
