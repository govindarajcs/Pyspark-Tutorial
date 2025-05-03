from pyspark import SparkConf
import configparser

class SparkConfig:

    @staticmethod
    def load_config(file_path):
        spark_conf = SparkConf()
        config = configparser.ConfigParser()
        config.read(file_path)
        for (config_key,config_value) in config.items("SPARK_APP_CONF"):
            print(f"Key : {config_key}")
            print(f"Value : {config_value}\n")
            spark_conf.set(config_key, config_value)
        return spark_conf

    @staticmethod
    def load_data(spark, file_path):
        return spark.\
                read.\
                csv(file_path, header=True, inferSchema=True)