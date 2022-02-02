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
)
from project_ops_next_analytics.utils.de_utils import pandas_to_pyspark

from pyspark import sql
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, DateType
import pandas as pd


def int_create_attendance_data(pandas_df: dict) -> sql.DataFrame:
    """
    This function reads attendance data and perform cleaning, data type conversion on it.
    Args:
        pandas_df: Attendance data.

    Returns: Cleaned attendance data

    """
    df = pd.concat(pandas_df, ignore_index=True)

    pandas_df = clean_pandas_column_names(df)

    outputschema = StructType(
        [
            StructField("person_id", StringType(), True),
            StructField("person_name", StringType(), True),
            StructField("date", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("employee_sub_group", StringType(), True),
            StructField("reporting_manager", StringType(), True),
            StructField("planned_shift_code", StringType(), True),
            StructField("actual_shift_code", StringType(), True),
            StructField("in", StringType(), True),
            StructField("out", StringType(), True),
            StructField("worked_hours", StringType(), True),
            StructField("extra_hours", StringType(), True),
            StructField("department", StringType(), True),
        ]
    )

    spark_df = pandas_to_pyspark(pandas_df, outputschema)

    df = clean_csvdata(spark_df)

    df = convert_to_date(df, {"date": "dd/MM/yyyy",},)

    return df
