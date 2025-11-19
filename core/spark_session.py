from pyspark.sql import SparkSession

def get_spark(conf,log4j_config_path,log_dir) : 
        spark_conf = conf["spark"]

        builder = (
                SparkSession.builder
                .appName(spark_conf.get("spark.app.name", "DefaultApp"))
        )

        # Apply spark configs one by one
        for key, value in spark_conf.items():
                builder = builder.config(key, value)

        # Log4j properties
        builder = builder.config("spark.driver.extraJavaOptions",
                        f"-Dlog4j.configuration=file:{log4j_config_path} -Dcustom.log.dir={log_dir}")
        builder = builder.config("spark.executor.extraJavaOptions",
                        f"-Dlog4j.configuration=file:{log4j_config_path} -Dcustom.log.dir={log_dir}")

        # defining jar packages
        builder = builder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.0.1")

        # enable hive support
        builder = builder.enableHiveSupport()

        spark = builder.getOrCreate()

        return spark