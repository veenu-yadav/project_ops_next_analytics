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
from project_ops_next_analytics.utils.data_clean import clean_csvdata
from pyspark import sql
from pyspark.sql import functions as F
from typing import Dict


def int_create_tfct_cqa_data(spark_df: sql.DataFrame, param: Dict,) -> sql.DataFrame:
    """
    This function reads tfct cqa data and perform cleaning, data type conversion on it.
    Args:
        spark_df: tfct cqa data
        param: parameters

    Returns: Cleaned tfct cqa data for a given plant.

    """
    plant_code = param["plant_code"]
    df = clean_csvdata(spark_df)

    df = df.filter(F.col("plant_code").isin(plant_code))

    df = convert_to_date(
        df,
        {
            "test_date": "yyyy-MM-dd HH:mm:ss",
            "mfg_date": "yyyy-MM-dd",
            "sample_registereddate": "yyyy-MM-dd HH:mm:ss",
            "lot_created_date": "yyyy-MM-dd HH:mm:ss",
            "ud_date": "yyyy-MM-dd",
            "coa_releaseddate": "yyyy-MM-dd HH:mm:ss",
            "last_mod_date": "yyyy-MM-dd HH:mm:ss",
            "coa_releaseddate_pk": "yyyy-MM",
        },
    )
    df = (
        df.withColumnRenamed("sample_registereddate", "sample_registered_date")
        .withColumnRenamed("coa_releaseddate", "coa_released_date")
        .withColumnRenamed("coa_releaseddate_pk", "coa_released_date_pk")
    )

    return df
