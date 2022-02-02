# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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
from typing import Dict
from project_ops_next_analytics.utils.de_utils import convert_to_date
from project_ops_next_analytics.utils.data_clean import (
    clean_csvdata,
    remove_special_character,
    create_substring,
    remove_brackets_character,
)
from pyspark import sql
import pyspark.sql.functions as F


def int_create_mes_pasx_ipc_data(
    spark_df: sql.DataFrame, param: Dict,
) -> sql.DataFrame:
    """
    This function reads ipc data from mes pasx and perform cleaning, data type conversion on it.
    Args:
        spark_df: ipc data from mes pasx
        param: parameters

    Returns: Cleaned ipc data from mes pasx for a given plant

    """
    plant_code = param["plant_code"]

    df = clean_csvdata(spark_df)
    df = df.filter(F.col("plant_code").isin(plant_code))

    df = convert_to_date(
        df,
        {
            "execution_start_time": "yyyy-MM-dd HH:mm:ss",
            "record_datetime_yyyy_mm": "yyyy-MM-dd HH:mm:ss",
        },
    )

    df = df.withColumnRenamed("record_datetime_yyyy_mm", "record_datetime")
    return df
