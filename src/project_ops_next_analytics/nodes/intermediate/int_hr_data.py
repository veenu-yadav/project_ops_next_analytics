# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited (“QuantumBlack”) name and logo
# (either separately or in combination, “QuantumBlack Trademarks”) are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Intermediate layer node for CSR data"""
from project_ops_next_analytics.utils.de_utils import convert_to_date
from project_ops_next_analytics.utils.data_clean import (
    clean_csvdata,
    clean_pandas_column_names,
    clean_pandas_date_format,
)
from project_ops_next_analytics.utils.de_utils import pandas_to_pyspark

from pyspark import sql
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, DateType


def int_create_transfer_emp_data(pandas_df: sql.DataFrame) -> sql.DataFrame:
    """
    This function reads transferred employee data and perform cleaning, data type conversion on it.
    Args:
        pandas_df: transferred employee data

    Returns: Cleaned transferred employee data.

    """
    pandas_df.columns = [
        "Employee No.",
        "Employee Name",
        "From  Unit",
        "From  Department",
        "To  Unit",
        "To Department",
        "Wef",
    ]
    pandas_df = clean_pandas_column_names(pandas_df)
    pandas_df = clean_pandas_date_format(pandas_df, ["wef"])

    outputschema = StructType(
        [
            StructField("employee_no", StringType(), True),
            StructField("employee_name", StringType(), True),
            StructField("from_unit", StringType(), True),
            StructField("from_department", StringType(), True),
            StructField("to_unit", StringType(), True),
            StructField("to_department", StringType(), True),
            StructField("wef", DateType(), True),
        ]
    )
    df = pandas_to_pyspark(pandas_df, outputschema)

    return df


def int_create_active_emp_data(pandas_df: sql.DataFrame) -> sql.DataFrame:
    """
    This function reads active employee data and perform cleaning, data type conversion on it.
    Args:
        pandas_df: active employee data

    Returns: Cleaned active employee data.

    """
    pandas_df = clean_pandas_column_names(pandas_df)

    outputschema = StructType(
        [
            StructField("emp_no", StringType(), True),
            StructField("emp_name", StringType(), True),
            StructField("employee_type", StringType(), True),
            StructField("personnel_areaorunit", StringType(), True),
            StructField("new_employee_group", StringType(), True),
            StructField("new_employee_group_1", StringType(), True),
            StructField("position_desc", StringType(), True),
            StructField("experience_in", StringType(), True),
            StructField("experience_out", StringType(), True),
            StructField("experience_total", StringType(), True),
            StructField("date_of_joining", DateType(), True),
            StructField("age", IntegerType(), True),
            StructField("all_qualifications", StringType(), True),
            StructField("highest_qualification", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("land1_empcode", StringType(), True),
            StructField("land1_name", StringType(), True),
            StructField("land2_name", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("leave_data", DoubleType(), True),
            StructField("rating_fy19", StringType(), True),
            StructField("fy20", StringType(), True),
            StructField("last_employer", StringType(), True),
            StructField("promotion", DateType(), True),
            StructField("bus", StringType(), True),
        ]
    )

    spark_df = pandas_to_pyspark(pandas_df, outputschema)
    df = (
        spark_df.withColumnRenamed("emp_no", "employee_no")
        .withColumnRenamed("personnel_areaorunit", "personnel_area_or_unit")
        .withColumnRenamed("date_of_birth", "dob")
        .withColumnRenamed("date_of_joining", "doj")
    )

    return df


def int_create_exit_emp_data(pandas_df: sql.DataFrame) -> sql.DataFrame:
    """
    This function reads exited employee data and perform cleaning, data type conversion on it.
    Args:
        pandas_df: exited employee data

    Returns: Cleaned exited employee data.

    """
    pandas_df = clean_pandas_column_names(pandas_df)
    pandas_df = clean_pandas_date_format(pandas_df, ["dor"])

    outputschema = StructType(
        [
            StructField("employee_no", IntegerType(), True),
            StructField("emp_name", StringType(), True),
            StructField("employee_type", StringType(), True),
            StructField("personnel_areaorunit", StringType(), True),
            StructField("employee_group", StringType(), True),
            StructField("new_employee_group", StringType(), True),
            StructField("position_discritpion", StringType(), True),
            StructField("in_exp", DoubleType(), True),
            StructField("exp_out", DoubleType(), True),
            StructField("doj", DateType(), True),
            StructField("age", IntegerType(), True),
            StructField("highest_qualification", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("land1_emp_code", StringType(), True),
            StructField("land1", StringType(), True),
            StructField("dor", DateType(), True),
            StructField("reason", StringType(), True),
            StructField("rating_fy19", StringType(), True),
            StructField("fy20", StringType(), True),
        ]
    )

    spark_df = pandas_to_pyspark(pandas_df, outputschema)
    df = clean_csvdata(spark_df)
    df = df.withColumnRenamed(
        "personnel_areaorunit", "personnel_area_or_unit"
    ).withColumnRenamed("position_discritpion", "position_description")

    return df
