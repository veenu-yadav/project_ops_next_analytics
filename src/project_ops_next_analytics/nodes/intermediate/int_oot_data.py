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


def int_create_oot_data(pandas_df: sql.DataFrame) -> sql.DataFrame:
    """
    This function reads invalid oot data and perform cleaning, data type conversion on it.
    Args:
        pandas_df: invalid oot data

    Returns: Cleaned invalid oot data.

    """
    pandas_df = clean_pandas_column_names(pandas_df)

    outputschema = StructType(
        [
            StructField("sl_no", IntegerType(), True),
            StructField("reported_on", DateType(), True),
            StructField("notification_no", IntegerType(), True),
            StructField("material_number", StringType(), True),
            StructField("material_description", StringType(), True),
            StructField("batch_number", StringType(), True),
            StructField("inspection_lot_no", LongType(), True),
            StructField("test1", StringType(), True),
            StructField("test", StringType(), True),
            StructField("oot_acceptance", StringType(), True),
            StructField("initial_result", StringType(), True),
            StructField("final_result", StringType(), True),
            StructField("object_status", StringType(), True),
            StructField("required_end_date", DateType(), True),
            StructField("notif_completion_da", DateType(), True),
            StructField("notif_description", StringType(), True),
            StructField("iporfporstabilityorrmorpm", StringType(), True),
            StructField("validor_invalid", StringType(), True),
            StructField("defect_location", StringType(), True),
        ]
    )

    spark_df = pandas_to_pyspark(pandas_df, outputschema)

    df = clean_csvdata(spark_df)

    df = (
        df.withColumnRenamed(
            "iporfporstabilityorrmorpm", "ip_or_fp_or_stability_or_rm_or_pm"
        )
        .withColumnRenamed("validor_invalid", "valid_or_invalid")
        .withColumnRenamed("notif_completion_da", "notif_completion_date")
    )

    return df
