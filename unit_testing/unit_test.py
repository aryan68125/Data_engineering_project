import unittest
import os 
import sys
# CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))  # one level higher
# if PROJECT_ROOT not in sys.path:
#     sys.path.insert(0, PROJECT_ROOT)
from pyspark.sql import SparkSession, Row
from transformations.dataframe_transformations import DataFrameTransformations
from .generate_dataframe import GenerateDataFrame
from datetime import date , datetime

import logging

class TestDataFrameTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("PySparkUnitTest")
            .master("local[2]")
            .getOrCreate()
        )
        cls.transformer = DataFrameTransformations(cls.spark)
        # Adding a test level logger
        logging.basicConfig(
            filename="unit_test_errors.log",
            level=logging.ERROR,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        cls.py_logger = logging.getLogger(__name__)


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # unit test methods here 
    def test_create_unique_identifier(self):
        try:
            data = [("A", 1, 1, 21), ("B", 2, 2, 22)]
            gen = GenerateDataFrame(self.spark)
            df = gen.generate_dataframe(data)

            result = self.transformer.create_unique_identifier(df)

            # Assert column 'id' exists
            self.assertIn("id", result.columns)

            # Assert values are unique
            ids = [row["id"] for row in result.collect()]
            self.assertEqual(len(ids), len(set(ids)))
        except Exception as e:
            self.py_logger.error(f"Error in test_create_unique_identifier: {str(e)}")
            raise

    
    def test_process_date_col_year(self):
        data = [
            (2021, 6, 5),
            (1985, 8, 10)
        ]
        df = self.spark.createDataFrame(data, ["year", "month", "day"])

        transformer = Transformations(self.spark)
        result = transformer.process_date_col_year(df, col_name="year", combine_date=True)
        rows = result.collect()

        # Check that dob is correct
        self.assertEqual(str(rows[0]["dob"]), "2021-06-05")
        self.assertEqual(str(rows[1]["dob"]), "1985-08-10")

        # Ensure old columns were dropped
        self.assertNotIn("year", result.columns)
        self.assertNotIn("month", result.columns)
        self.assertNotIn("day", result.columns)

        # Ensure new column exists
        self.assertIn("dob", result.columns)


    def test_drop_duplicate_rows(self):
        try:
            rows = [
                ("A", 1, 1, 21),
                ("A", 1, 1, 21),   # duplicate
                ("B", 2, 2, 22)
            ]
            gen = GenerateDataFrame(self.spark)
            df = gen.generate_dataframe(rows)

            result = self.transformer.drop_duplicate_rows(df, ["name", "day", "month", "year"])

            self.assertEqual(result.count(), 2)
        except Exception as e:
            self.py_logger.error(f"Error in test_drop_duplicate_rows: {str(e)}")
            raise

    def test_simple_aggregation_operation(self):
        try:
            rows = [
                Row(StockCode="A", Quantity=10, UnitPrice=2.5),
                Row(StockCode="B", Quantity=5, UnitPrice=1.0)
            ]
            gen = GenerateDataFrame(self.spark)
            df = gen.generate_dataframe(rows)

            result = self.transformer.simple_aggregation_operation(df)
            row = result.collect()[0]

            self.assertEqual(row["count"], 2)
            self.assertEqual(row["count field"], 2)
            self.assertEqual(row["TotalQuantity"], 15)
            self.assertAlmostEqual(row["AverageUnitPrice"], 1.75, places=2)
        except Exception as e:
            self.py_logger.error(f"Error in test_simple_aggregation_operation: {str(e)}")
            raise

    def test_complex_aggregation_operation(self):
        try:
            rows = [
                Row(Country="India", InvoiceNo="1", Quantity=10, UnitPrice=2),
                Row(Country="India", InvoiceNo="1", Quantity=5, UnitPrice=2),
            ]
            gen = GenerateDataFrame(self.spark)
            df = gen.generate_dataframe(rows)

            result = self.transformer.complex_aggregation_operation(df).collect()[0]

            self.assertEqual(result["TotalQuantity"], 15)
            self.assertEqual(result["InvoiceValue"], 30.00)
        except Exception as e:
            self.py_logger.error(f"Error in test_complex_aggregation_operation: {str(e)}")
            raise

    def test_group_by_country_agg(self):
        try:
            rows = [
                Row(Country="India", InvoiceNo="X1", InvoiceDate=datetime(2010, 1, 5), Quantity=10, UnitPrice=1),
                Row(Country="India", InvoiceNo="X2", InvoiceDate=datetime(2010, 1, 10), Quantity=20, UnitPrice=1),
            ]
            gen = GenerateDataFrame(self.spark)
            df = gen.generate_dataframe(rows)

            result = self.transformer.group_by_country_agg(df)

            self.assertEqual(result.count(), 2)
            self.assertIn("NumInvoice", result.columns)
            self.assertIn("TotalQuantity", result.columns)
            self.assertIn("InvoiceValue", result.columns)
        except Exception as e:
            self.py_logger.error(f"Error in test_group_by_country_agg: {str(e)}")
            raise

if __name__ == '__main__':
    unittest.main()
