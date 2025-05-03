from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("FirstSpark").getOrCreate()

sample_data = [("Raj", 20000), ("Uma", 15000), ("Mouleka", 10000)]

df = spark.createDataFrame(sample_data).toDF("Name", "Salary")

df.show()
spark.stop()