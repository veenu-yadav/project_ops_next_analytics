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
import pandas as pd
from pyspark.sql import functions as F
from project_ops_next_analytics.utils.de_utils import generate_dates
from project_ops_next_analytics.utils.data_clean import (
    remove_duplicates,
    format_col_values,
    add_prefix_to_colname,
)
from project_ops_next_analytics.custom_context import CustomContext


def create_master_timeseries(
    custom_context: CustomContext, equp_list, range_list, interval, dt_col
):
    """
    This function generates minute level time series in a given date range for each equipment.
    Args:
        custom_context: spark session.
        equp_list: list of equipments for which minute level time series has to be generated.
        range_list: list containing start date and end date.
        interval: time interval between date series (60 for minute series).
        dt_col: name of the outpt date column.

    Returns: minute level time series in a given date range for each equipment.

    """

    spark = custom_context
    master_dates = generate_dates(spark, range_list, interval, dt_col,)

    master_equp = pd.DataFrame({"master_equp_id": equp_list})
    master_equp_df = spark.createDataFrame(master_equp)

    master_df = master_dates.crossJoin(master_equp_df)

    return master_df


def select_cpp_data(cpp_df, tag_list):
    """
    This function pivots cpp tag names from the given tag list.
    Args:
        cpp_df: cpp data.
        tag_list: tag name sto be considered.

    Returns:
        pivot up cpp tag names from the given tag list.

    """

    cpp_df = remove_duplicates(
        cpp_df,
        ["cpp_plant_code", "cpp_equipment_id", "cpp_record_datetime", "cpp_tag_name"],
        ["cpp_tag_value"],
    )

    cpp_df = cpp_df.filter(F.col("cpp_tag_name").isin(tag_list))
    cpp_df = format_col_values(cpp_df, ["cpp_tag_name"])

    cpp_df = cpp_df.selectExpr(
        "cpp_plant_code",
        "cpp_record_datetime",
        "cpp_batch_id",
        "cpp_equipment_id as cpp_equp_id",
        "cpp_tag_name",
        "cpp_tag_value",
    ).withColumn(
        "cpp_record_datetime",
        F.to_timestamp(F.date_format(F.col("cpp_record_datetime"), "yyyy-MM-dd HH:mm")),
    )

    cpp_df = (
        cpp_df.groupby(
            "cpp_plant_code", "cpp_record_datetime", "cpp_batch_id", "cpp_equp_id",
        )
        .pivot("cpp_tag_name")
        .agg(F.first("cpp_tag_value"))
    )

    return add_prefix_to_colname(
        cpp_df,
        "cpp_param_",
        ["cpp_plant_code", "cpp_record_datetime", "cpp_batch_id", "cpp_equp_id"],
    )
