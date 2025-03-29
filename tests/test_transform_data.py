import unittest
from pyspark.sql import SparkSession
from scripts.transform_data import fetch_and_create_dataframe
from pyspark.sql.functions import col

class TestTransformData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestApp").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_fetch_and_create_dataframe(self):
        # Executa a função para criar os DataFrames
        fetch_and_create_dataframe()

        # Verificar se os DataFrames foram criados corretamente
        bronze_df = self.spark.read.json("gs://seu-bucket/bronze_breweries")
        self.assertIsNotNone(bronze_df)
        self.assertGreater(bronze_df.count(), 0) # Verificar se há dados

        silver_df = self.spark.read.parquet("gs://seu-bucket/silver_breweries")
        self.assertIsNotNone(silver_df)
        self.assertGreater(silver_df.count(), 0)

        gold_df = self.spark.read.parquet("gs://seu-bucket/gold_breweries")
        self.assertIsNotNone(gold_df)
        self.assertGreater(gold_df.count(), 0)

        # Verificar se as colunas esperadas estão presentes
        self.assertIn("id", bronze_df.columns)
        self.assertIn("name", silver_df.columns)
        self.assertIn("count", gold_df.columns)

        # Verificar se a partição por estado foi feita corretamente
        self.assertIn("state", silver_df.rdd.getNumPartitions())
