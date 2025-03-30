import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import StructType, StructField, StringType
from airflow.models import Variable

def fetch_data():
    url = "https://api.openbrewerydb.org/v1/breweries"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data:
            raise ValueError("Resposta da API está vazia")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados da API: {e}")
        return None

def fetch_and_create_dataframe():
    data = fetch_data()
    # Obter nome do bucket a partir das variáveis do Airflow
    gcs_bucket = Variable.get("gcs_bucket")
    spark = SparkSession.builder.appName("BreweryETL").getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_at", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)
    try:
        # Camada Bronze (dados brutos)
        df.write.json("gs://bees_case/bronze_breweries", mode='overwrite')
    
        # Camada Silver (exemplo transformações)
        df_silver = df.select("id", "name", "brewery_type", "city", "state", "country", "phone", "website_url")
        # Subtituindo nulos por "Not Available"
        for col_name in df_silver.columns:
            df_silver = df_silver.withColumn(col_name, when(col(col_name).isNull(), "Not Available").otherwise(col(col_name)))
        df_silver.write.parquet("gs://bees_case/silver_breweries", partitionBy="state", mode='overwrite')
    
        # Camada Gold (exemplo agregação)
        df_gold = df_silver.groupBy("state", "brewery_type").agg(
            count("*").alias("count")
        ).select("state", "brewery_type", "count").orderBy(col("count").desc())
        df_gold.write.parquet("gs://bees_case/gold_breweries", partitionBy="state", mode='overwrite')
    except Exception as e:
        print(f"Erro durante operação de escrita: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    gcs_bucket = Variable.get("gcs_bucket")
    fetch_and_create_dataframe()
