from pyspark.sql import SparkSession

def get_spark(conf,log4j_config_path,log_dir) : 
    spark = (
        SparkSession
        .builder
        .config(conf=conf)
        .config("spark.driver.extraJavaOptions",
                f"-Dlog4j.configuration=file:{log4j_config_path} -Dcustom.log.dir={log_dir}")
        .config("spark.executor.extraJavaOptions",
                f"-Dlog4j.configuration=file:{log4j_config_path} -Dcustom.log.dir={log_dir}")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.0.1")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark