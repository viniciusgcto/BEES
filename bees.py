from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import requests
# Função para consumir dados da API e criar DataFrame
def fetch_and_create_dataframe():
    # Consumindo a API Open Brewery DB
    url = 'https://api.openbrewerydb.org/breweries'
    response = requests.get(url)
    breweries = response.json()
    
    # Criando a sessão do Spark
    spark = SparkSession.builder.appName("BreweryData").getOrCreate()
    
    # Criando o DataFrame a partir dos dados da API
    df = spark.read.json(spark.sparkContext.parallelize([breweries]))
    
    # Persistindo os dados brutos na camada Bronze no GCS
    df.write.json("gs://bees_case/bronze_breweries", mode='overwrite')

# Função para transformar os dados, salvar em Parquet e particionar
def transform_and_store_in_parquet():
    # Criando a sessão do Spark
    spark = SparkSession.builder.appName("BreweryData").getOrCreate()
    
    # Carregando os dados da camada Bronze
    df = spark.read.json("gs://bees_case/bronze_breweries")
    
    # Transformação: particionando por cidade e salvando em Parquet
    df.write.partitionBy("city").parquet("gs://bees_case/silver_breweries", mode='overwrite')

# Função para realizar a agregação e salvar na camada Ouro
def create_aggregated_view():
    # Criando a sessão do Spark
    spark = SparkSession.builder.appName("BreweryData").getOrCreate()
    
    # Carregando os dados da camada Prata
    df = spark.read.parquet("gs://bees_case/silver_breweries")
    
    # Realizando agregação por tipo e cidade
    aggregated = df.groupBy("type", "city").count()
    
    # Salvando a visualização agregada na camada Ouro
    aggregated.write.parquet("gs://bees_case/gold_breweries", mode='overwrite')

# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 29),
}

dag = DAG('brewery_pipeline', default_args=default_args, schedule_interval='0 8 * * *')  # Agendamento para 8h da manhã diariamente

# Tarefas
task_fetch = PythonOperator(task_id='fetch_and_create_dataframe', python_callable=fetch_and_create_dataframe, dag=dag)
task_transform = PythonOperator(task_id='transform_data', python_callable=transform_and_store_in_parquet, dag=dag)
task_aggregate = PythonOperator(task_id='aggregate_data', python_callable=create_aggregated_view, dag=dag)

# Definindo a ordem das tarefas
task_fetch >> task_transform >> task_aggregate
