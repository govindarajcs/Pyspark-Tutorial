from pyspark.sql import *
from lib.logger import Log4j
from lib.config_utils import SparkConfig
import sys

if __name__ == '__main__':
    print("Welcome to Pyspark Programming")
    """spark = (SparkSession.builder
             .master("local[3]")
             .appName("Hello Spark")
             .getOrCreate()
             )"""
    if len(sys.argv)<3:
        print("ERROR: File name is missing\n CLI will be "
              "HelloSpark.py resources//<file_name>.csv resources//spark.conf")
        sys.exit(1)
    conf = SparkConfig.load_config(sys.argv[2])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting spark project")

    dataframe = SparkConfig.load_data(spark,sys.argv[1])
    dataframe.show()
    logger.info(f"Config: {conf.get("spark.test.name")}")
    logger.info("Finished spark project")