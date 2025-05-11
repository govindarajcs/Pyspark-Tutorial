from pyspark import SparkConf
import configparser

class SparkConfig:

    @staticmethod
    def load_config(file_path):
        spark_conf = SparkConf()
        config = configparser.ConfigParser()
        config.read(file_path)
        for (config_key,config_value) in config.items("SPARK_APP_CONF"):
            spark_conf.set(config_key, config_value)
        return spark_conf

    @staticmethod
    def load_data(spark, file_path):
        return spark \
            .read \
            .option("header", "true")\
            .option("inferSchema", "true")\
            .csv(file_path)

    @staticmethod
    def get_country_count_above_age_40(dataframe):
        return (dataframe
         .filter("Age < 40")
         .select("Age", "Country", "Gender", "State")
         .groupBy("Country")
         .count())

