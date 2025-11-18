from typing import List, Tuple, Any
from lib.logger import Log4j, LogSparkDataframe
from lib.app_monitor import GetDataFrameMemory
from pyspark.sql import functions as F
from pyspark.sql import Row 

class GenerateDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.logger = Log4j(spark)
        self.sp_df_logger = LogSparkDataframe(spark)
        self.app_metrics = GetDataFrameMemory(spark)

    def generate_dataframe(self, data_list: List[Any]):
        self.logger.debug("Checking for the supplied data_list...")

        if not data_list:
            self.logger.error("DataList required to generate a DataFrame!")
            raise ValueError("DataList required to generate a DataFrame!")

        first = data_list[0]

        # Case 1 → Row objects, keep schema
        if isinstance(first, Row):
            df = self.spark.createDataFrame(data_list)

        # Case 2 → Tuple input → name columns explicitly
        elif isinstance(first, tuple):
            num_cols = len(first)

            # If tuple length = 4, assume: name, day, month, year
            if num_cols == 4:
                columns = ["name", "day", "month", "year"]
            else:
                # fallback generic names c1, c2, c3...
                columns = [f"c{i}" for i in range(1, num_cols + 1)]

            df = self.spark.createDataFrame(data_list).toDF(*columns)

        else:
            raise TypeError("Unsupported data type. Use Row() or tuple.")

        # Repartition logic
        if self.spark.sparkContext.master == "local[3]":
            df = df.repartition(3)

        # Logging
        self.app_metrics.get_mem_usage(df)
        self.sp_df_logger.log_df_metrics(df, "generated_df")

        return df