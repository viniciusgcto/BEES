import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def fetch_and_create_dataframe():
    url = "https://api.openbrewerydb.org/breweries"
    response = requests.get(url)
    data = response.json()

    spark = SparkSession.builder.appName("BreweryETL").getOrCreate()

    # Especificar o schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_at", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)

    # Camada Bronze (dados brutos)
    df.write.json("gs://bees_case/bronze_breweries", mode='overwrite')

    # Camada Prata (exemplo transformações)
    df_silver = df.select("id", "name", "brewery_type", "city", "state", "country")
    df_silver.write.parquet("gs://bees_case/silver_breweries", partitionBy="state", mode='overwrite')

    # Camada Ouro (exemplo agregação)
    df_gold = df_silver.groupBy("state", "brewery_type").count()
    df_gold.write.parquet("gs://bees_case/gold_breweries", mode='overwrite')

    spark.stop()

if __name__ == "__main__":
    fetch_and_create_dataframe()
