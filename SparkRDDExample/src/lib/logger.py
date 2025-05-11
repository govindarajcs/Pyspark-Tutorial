class Log4j:

    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "guru.learningjournal.spark.examples"
        app_id = spark.sparkContext.getConf().get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class+"."+app_id)


    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)
