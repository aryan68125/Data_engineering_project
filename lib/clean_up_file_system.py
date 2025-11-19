import os
import shutil

import time

from lib.ENUM import EnvEnum

from lib.logger import PyLoggerRegistry

"""
This class will cleanup the data when the spark application re-runs

The files like logs, metastore , spark-warehouse etc.. will be cleaned up (deleted from the file system)
"""
class CleanupAppFileSystemOnReRun:
    def __init__(self,project_dir,env=None,conf=None):
        self.project_dir = project_dir
        self.env = env
        # Get the application configurations
        self.conf = conf
        # Get the log file name from the application configurations
        self.before_spark_initialization_logs = self.conf["custom"]["before_spark_initialization_logs"]
        self.py_logger = PyLoggerRegistry.get_logger(
            "main.CleanupAppFileSystemOnReRun",
            file=os.path.join(project_dir, self.before_spark_initialization_logs)
        )

    def execute_cleanup(self,clean_logs:bool = False):
        try:
            # This will prevent the cleanup process for the dataLake to take place when the application is running on databricks instead of local machine
            if self.env == EnvEnum.LOCAL_ENV.value:
                self.derby_logs_cleanup()
                self.spark_warehouse_cleanup()
                self.metastore_cleanup()
                if clean_logs == True:
                    self.logs_cleanup()
            elif self.env == EnvEnum.DATABRICKS_ENV.value:
                self.py_logger.info(f"📢️ App is running on databricks hence auto-clean dataLake, database metadata and logs will not be done")
                pass
            else:
                self.py_logger.info(f"📢️ auto-clean has detected some other environment other than databricks or local machine no cleaning action will be taken for safety measures")
                pass
            # time.sleep(5)
        except Exception as e:
            self.py_logger.error(f"❌ {str(e)}")
            raise

    """This will cleanup the derby.logs"""
    def derby_logs_cleanup(self):
        try:
            derby_logs_dir = os.path.join(self.project_dir, "derby.log")
            if os.path.exists(derby_logs_dir):
                os.remove(derby_logs_dir)
                self.py_logger.info(f"Deleted existing derby.log file: {derby_logs_dir}")
            else:
                self.py_logger.warning(f"⚠️ derby.log file does not exists: {derby_logs_dir}")
        except Exception as e:
            self.py_logger.error(f"❌ {str(e)}")
            raise
    
    """This will cleanup the spark_warehouse"""
    def spark_warehouse_cleanup(self):
        try:
            spark_warehouse_dir = os.path.join(self.project_dir, "spark-warehouse")
            if os.path.exists(spark_warehouse_dir):
                shutil.rmtree(spark_warehouse_dir)
                self.py_logger.info(f"Deleted existing spark-warehouse directory: {spark_warehouse_dir}")
            else:
                self.py_logger.warning(f"spark-warehouse directory does not exists: {spark_warehouse_dir}")
        except Exception as e:
            self.py_logger.error(f"❌ {str(e)}")
            raise

    """This will cleanup the metastore_db"""
    def metastore_cleanup(self):
        try:
            metastore_dir = os.path.join(self.project_dir, "metastore_db")
            if os.path.exists(metastore_dir):
                shutil.rmtree(metastore_dir)
                self.py_logger.info(f"Deleted existing metastore directory: {metastore_dir}")
            else:
                self.py_logger.warning(f"metastore directory does not exists: {metastore_dir}")
        except Exception as e:
            self.py_logger.error(f"❌ {str(e)}")
            raise

    """This will clean the logs folder"""
    def logs_cleanup(self):
        try:
            log_dir = os.path.join(self.project_dir, "log4j_properties", "logs")
            # delete the log directory
            if os.path.exists(log_dir):
                shutil.rmtree(log_dir)
                self.py_logger.info(f"Deleted existing log directory: {log_dir}")
            else:
                self.py_logger.warning(f"Log directory does not exist: {log_dir}")
        except Exception as e:
            self.py_logger.error(f"❌ {str(e)}")
            raise
            
