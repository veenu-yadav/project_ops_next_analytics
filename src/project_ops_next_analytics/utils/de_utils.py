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
"""Util functions for DE part of pipeline"""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import pandas as pd


def pandas_to_pyspark(data_frame: pd.DataFrame, outputSchema) -> DataFrame:
    """
    This function converts a pandas data frame to spark data frame.
        Args:
            data_frame: Pandas DataFrame
        Returns:
            Spark Data frame
    """
    ss = SparkSession.builder.getOrCreate()
    sdf = ss.createDataFrame(data_frame, schema=outputSchema)
    return sdf


def convert_to_date(spark_df: DataFrame, cols: dict):
    """
    This function converts given a date columns into "yyyy-MM-dd HH:mm:ss" format.
    Args:
        spark_df: spark data frame
        cols: the list of column to be converted into date format.

    Returns: Returns spark data frame with given date columns into "yyyy-MM-dd HH:mm:ss" format.

    """
    for dt_column, dt_fmt in cols.items():
        spark_df = spark_df.withColumn(
            dt_column, F.col(dt_column).cast(StringType())
        ).withColumn(
            dt_column,
            F.from_unixtime(
                F.unix_timestamp(dt_column, dt_fmt), "yyyy-MM-dd HH:mm:ss"
            ).cast("timestamp"),
        )
    return spark_df


def generate_dates(spark, range_list, interval, dt_col):
    """
    This function generates time series data frame in a given date range.
    :param spark: spark session
    :param range_list: Start Date
    :param interval: End Date
    :param dt_col: Date column

    :returns: time series data frame in a given date range.
    """
    start, stop = range_list
    temp_df = spark.createDataFrame([(start, stop)], ("start", "stop"))
    temp_df = temp_df.select([F.col(c).cast("timestamp") for c in ("start", "stop")])
    temp_df = temp_df.withColumn("stop", F.date_add("stop", 1).cast("timestamp"))
    temp_df = temp_df.select([F.col(c).cast("long") for c in ("start", "stop")])
    start, stop = temp_df.first()
    return spark.range(start, stop, interval).select(
        F.col("id").cast("timestamp").alias(dt_col)
    )


def generate_dates_24_hr(spark, range_list, interval, dt_col):
    """
    This function generates time series data frame in a given date range.
    :param spark: spark session
    :param range_list: Start Date
    :param interval: End Date
    :param dt_col: Date column

    :returns: time series data frame in a given date range.
    """
    start, stop = range_list

    temp_df = spark.createDataFrame([(start, stop)], ("start", "stop"))
    print(temp_df.show(10, False))

    temp_df = temp_df.select(
        [
            F.from_unixtime(F.unix_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss a"))
            .cast("timestamp")
            .alias(c)
            for c in ("start", "stop")
        ]
    )

    print(temp_df.show(10))

    temp_df = temp_df.withColumn("stop", F.date_add("stop", 1).cast("timestamp"))
    temp_df = temp_df.select([F.col(c).cast("long") for c in ("start", "stop")])
    start, stop = temp_df.first()
    return spark.range(start, stop, interval).select(
        F.col("id").cast("timestamp").alias(dt_col)
    )
