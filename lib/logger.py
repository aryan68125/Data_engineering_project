# imports related to implementing decorator for log4j 
import time 
import traceback

import logging
from typing import Dict, Optional

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
class PyLoggerRegistry:
    """
    Central registry for named loggers.


    Ensures:
    - No duplicate handlers
    - Consistent formatting
    - Easy access through get_logger(name)
    """

    _loggers : Dict[str, logging.Logger] = {}
    _default_level = logging.INFO
    _default_format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"

    @classmethod
    def get_logger(
        cls,
        name:str,
        level:Optional[int] = None,
        file:Optional[str] = None,
    ) -> logging.Logger:
        """
        Returns a configured logger. Creates a new one if not already registered.
        """
        if name in cls._loggers:
            return cls._loggers[name]
        logger = logging.getLogger(name)
        logger.setLevel(level or cls._default_level)
        logger.propagate = False # Stop double logging

        formatter = logging.Formatter(cls._default_format)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if file:
            file_handler = logging.FileHandler(file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        cls._loggers[name] = logger
        return logger

class PyLogger:
    """
    Simple wrapper class for a single logger instance.
    Provides clean .info(), .error(), .debug(), etc.
    """

    def __init__(self, name: str, level: Optional[int] = None, file: Optional[str] = None):
        self._logger = PyLoggerRegistry.get_logger(name, level, file)


    def info(self, msg: str, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)


    def warning(self, msg: str, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)


    def error(self, msg: str, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)


    def debug(self, msg: str, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs)


    def critical(self, msg: str, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)

