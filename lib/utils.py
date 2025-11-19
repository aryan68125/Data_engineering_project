import configparser
from pyspark import SparkConf
import os

# import python's logger for logging purposes
from lib.logger import PyLoggerRegistry
from lib.ENUM import EnvEnum
# import reading conf realted stuff
from core.config_loader import LoadAppConfigs

"""
This function will load the configuration from spark.conf file and return a spark conf object
"""
class DynamicAppConfigLoader:
    def __init__(self,env=None,project_dir=None):
        self.env = env

        # get yaml_config_path
        self.yaml_config_path = os.path.join(project_dir,"config", "app_config.yml")

        # setting up pylogger
        # Get the log file name from the application configurations
        self.py_logger = PyLoggerRegistry.get_logger(
            "main.DynamicAppConfigLoader",
            file=os.path.join(project_dir, "before_spark_initialization_logs.log")
        )
        print(f"DynamicAppConfigLoader | logfile path ===> {os.path.join(project_dir, "before_spark_initialization_logs.log")}")

        # application config loader from yaml file
        self.load_app_conf_obj = LoadAppConfigs(env=env, config_path=self.yaml_config_path)

    def get_spark_app_config(self):
        # If the application is not running on databricks and is running on a local machine 
        spark_conf = self.load_app_conf_obj.load_app_config()

        self.py_logger.info(f"💡️ Spark configs --> {spark_conf}")

        # [OLD CODE : DEPRICATED]
        # spark_conf = SparkConf()
        # config = configparser.ConfigParser()
        # # Read the spark.conf file from the dirtectory
        # config.read(os.path.join(os.getcwd(),"spark.conf"))

        # # Loop through the configs and set it to the spark conf
        # for (key, val) in config.items("SPARK_APP_CONFIGS"):
        #     spark_conf.set(key,val)
        return spark_conf