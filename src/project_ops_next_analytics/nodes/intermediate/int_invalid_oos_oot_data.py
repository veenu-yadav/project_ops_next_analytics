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


def int_create_invalid_oos_oot_data(spark_df: sql.DataFrame) -> sql.DataFrame:
    """
    This function reads invalid oos & oot data and perform cleaning, data type conversion on it.
    Args:
        spark_df: invalid oos & oot data

    Returns: Cleaned invalid oos & oot data.

    """
    df = clean_csvdata(spark_df)
    df = remove_pattern_from_column(df, "tfct_qms_")
    df = convert_to_date(
        df,
        {
            "created_date_time": "yyyy-MM-dd HH:mm:ss",
            "notification_closed_date_time": "yyyy-MM-dd HH:mm:ss",
            "planned_finish_date_time": "yyyy-MM-dd HH:mm:ss",
            "task_created_date_time": "yyyy-MM-dd HH:mm:ss",
            "task_closure_date_time": "yyyy-MM-dd HH:mm:ss",
            "task_closure_date_time": "yyyy-MM-dd HH:mm:ss",
            "last_mod_date": "yyyy-MM-dd HH:mm:ss",
        },
    )
    return df
