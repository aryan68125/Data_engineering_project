import configparser
from pyspark import SparkConf
import os

import logger

"""
This function will load the configuration from spark.conf file and return a spark conf object
"""
def get_spark_app_config(env=None):
    # If the application is not running on databricks and is running on a local machine 
    if not env:

    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    # Read the spark.conf file from the dirtectory
    config.read(os.path.join(os.getcwd(),"spark.conf"))

    # Loop through the configs and set it to the spark conf
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)
    return spark_conf