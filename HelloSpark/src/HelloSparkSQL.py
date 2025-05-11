import sys

from pyspark.sql import SparkSession
from lib.config_utils import SparkConfig


if __name__ == '__main__':
    conf = SparkConfig.load_config(sys.argv[2])

    spark = SparkSession.builder.config(conf=conf).appName("HelloSparkSQL").master("local[*]").getOrCreate()

    dataframe = SparkConfig.load_data(spark, sys.argv[1])
    dataframe.createOrReplaceTempView("US_Citizen_view")
    result = spark.sql("select Country, count(*) from US_Citizen_view as us_view where us_view.age<40 group by Country")
    result.show()

