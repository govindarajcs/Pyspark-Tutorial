from unittest import TestCase
from pyspark.sql import SparkSession

from HelloSpark.src.lib.config_utils import SparkConfig


class TestConfig(TestCase):

    @classmethod
    def setUp(cls):
        #cls.spark = SparkSession.builder.master("local[*]").appName("Hello PySpark Test").getOrCreate()
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("test_session") \
            .getOrCreate()

    def test_load_data(self):
        df = SparkConfig.\
            load_data(TestConfig.spark, "resources/USA_Citizenship.csv")
        self.assertEqual(df.count(), 9, "Load data is equal")

    def test_apply_filters(self):
        df = SparkConfig. \
            load_data(TestConfig.spark, "resources/USA_Citizenship.csv")
        df = SparkConfig.get_country_count_above_age_40(df)
        country_dict = {}
        for row in df.collect():
            country_dict[row['Country']] = row['count']
        self.assertEqual(country_dict['United States'], 4)
        self.assertEqual(country_dict['United Kingdom'], 1)
        self.assertEqual(country_dict['Canada'], 2)
