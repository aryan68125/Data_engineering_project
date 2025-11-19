# imports related to implementing decorator for log4j 
import time 
import traceback

import logging
from lib.ENUM import PyLogTypeEnum

class Log4j:
    def __init__(self, spark):
        # Get a log4j instance
        log4j = spark._jvm.org.apache.log4j
        # Create a logger attribute
        # put your organization name as a root class 
        root_class = "credencys.aditya.spark"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self,message):
        self.logger.warn(message)
    
    def info(self,message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def debug(self,message):
        self.logger.debug(message)

class LogSparkDataframe:
    def __init__(self,spark):
        self.sp = spark
        self.logger = Log4j(spark)
    # This will log the dataframe
    def log_df(self,spark_df,spark_df_name):
        self.logger.info(f"👉️ {spark_df_name} dataframe:\n{spark_df.limit(25).toPandas().to_string(index=False)}")
    def log_df_basic(self, spark_df, spark_df_name):
        rows = spark_df.count()
        cols = len(spark_df.columns)
        self.logger.debug(f"{spark_df_name}: rows={rows}, cols={cols}")
    # This will log the database schema
    def log_df_metrics(self,spark_df,spark_df_name):
        schema_str = spark_df._jdf.schema().treeString()
        self.logger.debug(f"{spark_df_name} :: operation - LogSparkDataframe :: Spark DataFrame Schema (expanded): {schema_str}")


def log_operation(fn):
    def wrapper(self, *args, **kwargs):
        logger = self.logger     # Log4j instance
        operation = fn.__name__  # name of the function being executed

        logger.info(f"▶️ Starting operation: {operation}")

        start = time.time()
        try:
            # Execute the original transformation function
            result = fn(self, *args, **kwargs)

            elapsed = round(time.time() - start, 3)
            logger.info(f"✅ Completed operation: {operation} in {elapsed}s")
            return result

        except Exception as e:
            logger.error(f"❌ Failed operation: {operation} | Error: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    return wrapper 

# This class will use python's inbuilt logger where Log4j couldn't be used
class PyLogger:
    def __init__(self,log_file_name=None):
        self.log_file_name = log_file_name

    def get_py_logger(self, log_type):
        if not self.log_file_name:
            raise ValueError(f"❌ log_file_name is required to be able to generate logs")
        
        if log_type == PyLogTypeEnum.LOG_ERROR:
            self.log_error()
        elif log_type == PyLogTypeEnum.LOG_WARNING:
            self.log_warning()
        elif log_type == PyLogTypeEnum.LOG_DEBUG:
            self.log_debug()
        elif log_type == PyLogTypeEnum.LOG_INFO:
            self.log_info()
        self.py_logger = logging.getLogger(__name__)
        return self.py_logger
    
    def log_error(self):
        logging.basicConfig(
                filename=self.log_file_name,
                level=logging.ERROR,
                format="%(asctime)s - %(levelname)s - %(message)s"
            )
    
    def log_warning(self):
        logging.basicConfig(
                filename=self.log_file_name,
                level=logging.WARNING,
                format="%(asctime)s - %(levelname)s - %(message)s"
            )
    
    def log_debug(self):
        logging.basicConfig(
                filename=self.log_file_name,
                level=logging.DEBUG,
                format="%(asctime)s - %(levelname)s - %(message)s"
            ) 
        
    def log_info(self):
        logging.basicConfig(
                filename=self.log_file_name,
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(message)s"
            )

