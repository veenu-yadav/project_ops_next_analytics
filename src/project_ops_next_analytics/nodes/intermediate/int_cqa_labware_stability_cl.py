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
from project_ops_next_analytics.utils.data_clean import remove_pattern_from_column
from project_ops_next_analytics.utils.data_clean import clean_csvdata
from pyspark import sql
from typing import Dict
from pyspark.sql import functions as F


def int_create_cqa_labware_stability_cl_data(
    spark_df: sql.DataFrame, param: Dict,
) -> sql.DataFrame:
    """
    This function reads cqa labware stability data and perform cleaning, data type conversion on it.
    Args:
        spark_df: cqa labware stability data
        param: parameters

    Returns: Cleaned cqa labware stability data for a given plant.

    """
    plant_code = param["plant_code"]
    df = clean_csvdata(spark_df)
    df = df.filter(F.col("plant_code").isin(plant_code))
    df = convert_to_date(
        df,
        {
            "mfg_date": "yyyy-MM-dd HH:mm:ss",
            "expiry_date": "yyyy-MM-dd HH:mm:ss",
            "study_start_date": "yyyy-MM-dd HH:mm:ss",
            "due_date": "yyyy-MM-dd HH:mm:ss",
            "available_date": "yyyy-MM-dd HH:mm:ss",
            "required_date": "yyyy-MM-dd HH:mm:ss",
            "target_date": "yyyy-MM-dd HH:mm:ss",
            "required_pull_date": "yyyy-MM-dd HH:mm:ss",
            "pull_date": "yyyy-MM-dd HH:mm:ss",
            "analysis_batch_num": "yyyy-MM-dd HH:mm:ss",
            "test_start_date": "yyyy-MM-dd HH:mm:ss",
            "test_completed_date": "yyyy-MM-dd HH:mm:ss",
            "sample_received_date": "yyyy-MM-dd HH:mm:ss",
            "sample_completed_date": "yyyy-MM-dd HH:mm:ss",
            "test_submitted_date": "yyyy-MM-dd HH:mm:ss",
            "review_date": "yyyy-MM-dd HH:mm:ss",
            "sample_approved_date": "yyyy-MM-dd HH:mm:ss",
            "data_extracted_on": "yyyy-MM-dd HH:mm:ss",
            "sample_registered_on": "yyyy-MM-dd HH:mm:ss",
            "investigation_created_on": "yyyy-MM-dd HH:mm:ss",
            "timepoint_comment_on": "yyyy-MM-dd HH:mm:ss",
        },
    )

    return df
