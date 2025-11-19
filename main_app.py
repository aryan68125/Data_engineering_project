from core.spark_session import get_spark
# import related to logging
from lib.logger import Log4j, LogSparkDataframe
# import related to custom spark configurations
from lib.utils import DynamicAppConfigLoader
# imports related to exporting dataframe
from lib.write_df import ExportSparkDataFrame
# import writing sparkdf to tables related stuff
from lib.load_df_data_into_table import LoadSparkDFIntoTable
# logging related imports 
import os

# Imports related to ingest data
from lib.ingest_data import IngestData
# Transform data
from transformations.dataframe_transformations import DataFrameTransformations

# imports related to cleanup when the main_app.py is re-run
from lib.clean_up_file_system import CleanupAppFileSystemOnReRun

# imports related to generating dataFrame
from unit_testing.generate_dataframe import GenerateDataFrame

# import enum
from lib.ENUM import EnvEnum

# import app yaml config loader
from core.config_loader import LoadAppConfigs

class MainDriver:
    def __init__(self):
        # detect environment 
        # This code will detect wheather the application is running on databricks or on a local machine
        self.env = EnvEnum.DATABRICKS_ENV.value if "DATABRICKS_RUNTIME_VERSION" in os.environ else EnvEnum.LOCAL_ENV.value
        
        # get project directory 
        self.project_dir = os.path.dirname(os.path.abspath(__file__))

        # initialize DynamicAppConfigLoader and get the application configuration depending on the env its running on
        d_app_conf_loader = DynamicAppConfigLoader(env = self.env, project_dir = self.project_dir)
        self.conf = d_app_conf_loader.get_spark_app_config()

    # main driver method 
    def main_driver(self):
        # Call all the methods one by one here
        self.clean_datalake_metadata_and_logs_before_re_run()
        self.setup_log4j_logger()
        self.create_spark_session()
        self.create_dataframe()
        self.deploy_transformations()
        self.ingest_data()
        self.deploy_transformations_for_ingest_data() 
        self.spark.stop()       

    # main functionality methods
    def clean_datalake_metadata_and_logs_before_re_run(self):
        cleanup = CleanupAppFileSystemOnReRun(project_dir=self.project_dir,env = self.env, conf = self.conf)
        cleanup.execute_cleanup(clean_logs=True)

    def setup_log4j_logger(self):
        # get log4j.properties config path
        env_log4j_properties_dir = self.conf["paths"]["log4j_file"]
        self.log4j_config_path = os.path.join(self.project_dir, env_log4j_properties_dir)
        # get log_dir_path
        env_log_dir = self.conf["paths"]["log_dir"]
        self.log_dir = os.path.join(self.project_dir, env_log_dir)
        # create log dir if not there
        os.makedirs(self.log_dir, exist_ok=True)

    def create_spark_session(self):
        self.spark = get_spark(conf=self.conf, log4j_config_path=self.log4j_config_path, log_dir=self.log_dir)

        # initialize logger class 
        self.logger = Log4j(self.spark)

        # initialize the spark dataframe logger 
        self.sp_df_logger = LogSparkDataframe(self.spark)

    def create_dataframe(self):
        self.logger.debug(f"log4j.properties file dir = {self.log4j_config_path}")
        self.logger.debug(f"log files dir = {self.log_dir}")
        self.logger.debug(f"log dir exists = {os.path.exists(self.log_dir)}")
        self.logger.info("Reading the data from the directory")
        self.dataset_dir = os.path.join(self.project_dir,"dataset")

        gen_df = GenerateDataFrame(self.spark)
        data_list = [
            ("Rollex","28","1","2002"),
            ("Ballistic","23","5","81"),
            ("Shotgun","12","12","6"),
            ("Artillery","7","8","63"),
            ("Ballistic","23","5","81"),
        ]
        self.generated_df = gen_df.generate_dataframe(data_list)
        self.sp_df_logger.log_df(spark_df=self.generated_df,spark_df_name="generated_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=self.generated_df,spark_df_name="generated_df")

    def deploy_transformations(self):
        # initialize df transformation class
        self.df_t = DataFrameTransformations(self.spark)
        # initialize df export class
        self.df_exp = ExportSparkDataFrame(self.spark)

        # add a uniquely identifiable id for the rows
        self.generated_df = self.df_t.create_unique_identifier(spark_df=self.generated_df) 
        # log the output dataframe
        self.sp_df_logger.log_df(spark_df=self.generated_df,spark_df_name="generated_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=self.generated_df,spark_df_name="generated_df")
        self.sp_df_logger.log_df_metrics(spark_df=self.generated_df,spark_df_name="generated_df")

        # process date and make all the inconsitent two digit and three digit year into 4 digit year
        processed_date_df = self.df_t.process_date_col_year(spark_df=self.generated_df,col_name="year",combine_date=True)
        # log the output dataframe
        self.sp_df_logger.log_df(spark_df=processed_date_df,spark_df_name="processed_date_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=processed_date_df,spark_df_name="processed_date_df")
        self.sp_df_logger.log_df_metrics(spark_df=processed_date_df,spark_df_name="processed_date_df")

        # process duplicate data in the dataFrame
        processed_duplicate_df = self.df_t.drop_duplicate_rows(spark_df=processed_date_df,col_name_list=["name","dob"])
        # log the output dataframe
        self.sp_df_logger.log_df(spark_df=processed_duplicate_df,spark_df_name="processed_duplicate_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=processed_duplicate_df,spark_df_name="processed_duplicate_df")
        self.sp_df_logger.log_df_metrics(spark_df=processed_duplicate_df,spark_df_name="processed_duplicate_df")

    def ingest_data(self):
        # get the file directory from where the data will be ingested 
        file_dir = os.path.join(self.dataset_dir,"invoices.csv")
        # initialize the ingest data class 
        ingest_data = IngestData(self.spark)
        self.spark_df =ingest_data.import_data_csv(file_dir=file_dir)
        # log the output dataframe
        self.sp_df_logger.log_df(spark_df=self.spark_df,spark_df_name="spark_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=self.spark_df,spark_df_name="spark_df")

    def deploy_transformations_for_ingest_data(self):
        # performing simple aggregation
        aggregated_df = self.df_t.simple_aggregation_operation(spark_df=self.spark_df)
        self.sp_df_logger.log_df(spark_df=aggregated_df,spark_df_name="aggregated_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=aggregated_df,spark_df_name="aggregated_df")

        # performing complex aggregation
        complex_aggregated_df = self.df_t.complex_aggregation_operation(spark_df=self.spark_df)
        self.sp_df_logger.log_df(spark_df=complex_aggregated_df,spark_df_name="complex_aggregated_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=complex_aggregated_df,spark_df_name="complex_aggregated_df")

        # performaing groupby "Country" and "WeekNumber" then perform aggregation operation on the dataFrame
        result_df = self.df_t.group_by_country_agg(spark_df=self.spark_df)
        self.logger.info('group the data based on "Country" and "WeekNumber" then perform aggregation operation on the dataFrame')
        self.sp_df_logger.log_df(spark_df=result_df,spark_df_name="result_df")
        # log the no of rows and columns in the dataframe
        self.sp_df_logger.log_df_basic(spark_df=result_df,spark_df_name="result_df")
        # export this df in a paraquet file
        output_path = os.path.join(self.project_dir,self.conf["paths"]["export_dir"])
        self.df_exp.export_df_parquet(spark_df=result_df,output_path=output_path)

        # Window aggregation implementation
        window_agg_result_df = self.df_t.window_aggregation(spark_df=self.spark_df)
        self.logger.info("Performaing window aggregation on the dataFrame")
        self.sp_df_logger.log_df(spark_df=window_agg_result_df,spark_df_name="window_agg_result_df")
        self.sp_df_logger.log_df_basic(spark_df=window_agg_result_df,spark_df_name="window_agg_result_df")

if __name__ == "__main__":

    driver = MainDriver()
    driver.main_driver()

    # check if the application is running on databricks or not 
    """change this : DONE"""
    # is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    # logging related logic
    # Get the current project's directory
    # project_dir = os.path.dirname(os.path.abspath(__file__))
    # cleanup loggic on main_app.py re-run
    # initialize the cleanup class
    """change this"""
    # cleanup = CleanupAppFileSystemOnReRun(project_dir,is_databricks)
    # cleanup.execute_cleanup(clean_logs=True)

    # Get the Log4j.properties file directory
    # log4j_config_path = os.path.join(project_dir, "log4j_properties", "log4j.properties")
    # Save the directory where the generated log files must reside
    # log_dir = os.path.join(project_dir, "log4j_properties", "logs")
    # Create the directory where the log files must be kept if not present
    # os.makedirs(log_dir, exist_ok=True)

    """change this"""
    # conf = get_spark_app_config(is_databricks=is_databricks)
    # spark = get_spark(conf=conf, log4j_config_path=log4j_config_path, log_dir=log_dir)

    

    # logging some debug related stuff 
    # logger.debug(f"log4j.properties file dir = {log4j_config_path}")
    # logger.debug(f"log files dir = {log_dir}")
    # logger.debug(f"log dir exists = {os.path.exists(log_dir)}")
    
    # logger.info("Reading the data from the directory")
    # dataset_dir = os.path.join(project_dir,"dataset")

    ######################################
    # CREATE A DATAFRAME AND PERFORM TRANSFORAMTION ON IT STARTS
    ######################################
    """Create DataFrame STARTS"""
    # gen_df = GenerateDataFrame(spark)
    # data_list = [
    #     ("Rollex","28","1","2002"),
    #     ("Ballistic","23","5","81"),
    #     ("Shotgun","12","12","6"),
    #     ("Artillery","7","8","63"),
    #     ("Ballistic","23","5","81"),
    # ]
    # generated_df = gen_df.generate_dataframe(data_list)
    # sp_df_logger.log_df(spark_df=generated_df,spark_df_name="generated_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=generated_df,spark_df_name="generated_df")
    """Create DataFrame ENDS""" 

    """Transformation STARTS"""
    # # initialize df transformation class
    # df_t = DataFrameTransformations(spark)
    # # initialize df export class
    # df_exp = ExportSparkDataFrame(spark)

    # # add a uniquely identifiable id for the rows
    # generated_df = df_t.create_unique_identifier(spark_df=generated_df) 
    # # log the output dataframe
    # sp_df_logger.log_df(spark_df=generated_df,spark_df_name="generated_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=generated_df,spark_df_name="generated_df")
    # sp_df_logger.log_df_metrics(spark_df=generated_df,spark_df_name="generated_df")

    # # process date and make all the inconsitent two digit and three digit year into 4 digit year
    # processed_date_df = df_t.process_date_col_year(spark_df=generated_df,col_name="year",combine_date=True)
    # # log the output dataframe
    # sp_df_logger.log_df(spark_df=processed_date_df,spark_df_name="processed_date_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=processed_date_df,spark_df_name="processed_date_df")
    # sp_df_logger.log_df_metrics(spark_df=processed_date_df,spark_df_name="processed_date_df")

    # # process duplicate data in the dataFrame
    # processed_duplicate_df = df_t.drop_duplicate_rows(spark_df=processed_date_df,col_name_list=["name","dob"])
    # # log the output dataframe
    # sp_df_logger.log_df(spark_df=processed_duplicate_df,spark_df_name="processed_duplicate_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=processed_duplicate_df,spark_df_name="processed_duplicate_df")
    # sp_df_logger.log_df_metrics(spark_df=processed_duplicate_df,spark_df_name="processed_duplicate_df")
    """Transformation ENDS"""
    ######################################
    # CREATE A DATAFRAME AND PERFORM TRANSFORAMTION ON IT ENDS
    ######################################






    ######################################
    # INGEST DATA INTO THE DATAFRAME AND PERFORM AGGREGATION OPERATIONS ON IT STARTS
    ######################################
    """Ingest data STARTS"""
    # # get the file directory from where the data will be ingested 
    # file_dir = os.path.join(dataset_dir,"invoices.csv")
    # # initialize the ingest data class 
    # ingest_data = IngestData(spark)
    # spark_df =ingest_data.import_data_csv(file_dir=file_dir)
    # # log the output dataframe
    # sp_df_logger.log_df(spark_df=spark_df,spark_df_name="spark_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=spark_df,spark_df_name="spark_df")
    """Ingest data ENDS"""


    """Perform aggregation operation on the dataFrame STARTS"""
    # # performing simple aggregation
    # aggregated_df = df_t.simple_aggregation_operation(spark_df=spark_df)
    # sp_df_logger.log_df(spark_df=aggregated_df,spark_df_name="aggregated_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=aggregated_df,spark_df_name="aggregated_df")

    # # performing complex aggregation
    # complex_aggregated_df = df_t.complex_aggregation_operation(spark_df=spark_df)
    # sp_df_logger.log_df(spark_df=complex_aggregated_df,spark_df_name="complex_aggregated_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=complex_aggregated_df,spark_df_name="complex_aggregated_df")

    # # performaing groupby "Country" and "WeekNumber" then perform aggregation operation on the dataFrame
    # result_df = df_t.group_by_country_agg(spark_df=spark_df)
    # logger.info('group the data based on "Country" and "WeekNumber" then perform aggregation operation on the dataFrame')
    # sp_df_logger.log_df(spark_df=result_df,spark_df_name="result_df")
    # # log the no of rows and columns in the dataframe
    # sp_df_logger.log_df_basic(spark_df=result_df,spark_df_name="result_df")
    # # export this df in a paraquet file
    # output_path = os.path.join(project_dir,"export")
    # df_exp.export_df_parquet(spark_df=result_df,output_path=output_path)

    # # Window aggregation implementation
    # window_agg_result_df = df_t.window_aggregation(spark_df=spark_df)
    # logger.info("Performaing window aggregation on the dataFrame")
    # sp_df_logger.log_df(spark_df=window_agg_result_df,spark_df_name="window_agg_result_df")
    # sp_df_logger.log_df_basic(spark_df=window_agg_result_df,spark_df_name="window_agg_result_df")
    """Perform aggregation operation on the dataFrame ENDS"""
    ######################################
    # INGEST DATA INTO THE DATAFRAME AND PERFORM AGGREGATION OPERATIONS ON IT ENDS
    ######################################

    # This line is for debugging only comment after <required to see the partitions of spark dataFrame>
    # input("Please enter")
    # spark.stop()




    
