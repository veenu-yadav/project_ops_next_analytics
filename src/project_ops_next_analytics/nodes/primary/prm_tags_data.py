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
from typing import Dict
import pandas as pd
from project_ops_next_analytics.utils.de_utils import generate_dates
from project_ops_next_analytics.utils.data_clean import (
    remove_duplicates,
    format_col_values,
    add_prefix_to_colname,
)
from pyspark.sql import functions as F
from pyspark import sql
from project_ops_next_analytics.nodes.primary.prm_equipment_details import spark


def create_master_timeseries(spark, equp_list, range_list, interval, dt_col):
    """
    This function generates minute level time series in a given date range for each equipment.
    Args:
        spark: spark session
        equp_list: list of equipments for which minute level time series has to be generated.
        range_list: list containing start date and end date
        interval: time interval between date series (60 for minute series)
        dt_col: name of the outpt date column

    Returns:
        minute level time series in a given date range for each equipment.

    """
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


def select_ems_data(ems_df, equip_room_df):
    """
    This function returns equipment to room temperature and relative humidity mapping.
    Args:
        ems_df: ems data with room temperature and relative humidity mapping.
        equip_room_df: room to eaquipment mapping.

    Returns: Returns equipment to room temperature and relative humidity mapping.

    """
    ems_df = ems_df.withColumn(
        "ems_record_datetime",
        F.to_timestamp(F.date_format(F.col("date_and_time"), "yyyy-MM-dd HH")),
    )

    ems_df = remove_duplicates(
        ems_df, ["ems_record_datetime", "room_info"], ["temperaturenmt_25_deg_c"],
    ).filter((F.col("date_and_time").isNotNull()))

    ems_df = ems_df.select(
        "ems_record_datetime",
        "room_id",
        "temperaturenmt_25_deg_c",
        "relative_humiditynmt_50_percent",
    )

    equip_room_df = equip_room_df.withColumnRenamed("equipment", "ems_equp_id")

    ems_join_equipment = ems_df.join(
        equip_room_df, ems_df.room_id == equip_room_df.area, "inner"
    ).select(
        "ems_record_datetime",
        "ems_equp_id",
        "temperaturenmt_25_deg_c",
        "relative_humiditynmt_50_percent",
    )

    return add_prefix_to_colname(
        ems_join_equipment, "ems_var_", ["ems_record_datetime", "ems_equp_id", ]
    )


def prm_create_tags_data(
    cpp_df: sql.DataFrame,
    ems_df: sql.DataFrame,
    equip_room_df: sql.DataFrame,
    param: Dict,
) -> sql.DataFrame:
    """
    This function generates minute level time series data for a given date range mapped with corresponding cpp and ems
     information for each equipment.

    Args:
        cpp_df: cpp sensor data.
        ems_df: ems data
        equip_room_df: equipment room mapping data.
        param: parameters

    Returns:
        Minute level time series data for a given date range mapped with corresponding cpp and ems for each equipment.

    """

    equp_list = param["equp_list"]
    cpp_tags_start_date = param["tags"]["tags_start_date"]
    cpp_tags_end_date = param["tags"]["tags_end_date"]
    tag_list = param["cpp_tags"]["tags"]

    # Generate minute level timeseries for a given date range for each equipment.
    master_df = create_master_timeseries(
        spark,
        equp_list,
        range_list=[cpp_tags_start_date, cpp_tags_end_date],
        interval=60,
        dt_col="master_record_datetime",
    )

    # format cpp data
    cpp_df = select_cpp_data(cpp_df, tag_list)
    # format ems data
    ems_df = select_ems_data(ems_df, equip_room_df)

    # Join minute level time series data to cpp data
    master_cpp_join = (
        master_df.join(
            cpp_df,
            (
                (master_df.master_record_datetime == cpp_df.cpp_record_datetime)
                & (master_df.master_equp_id == cpp_df.cpp_equp_id)
            ),
            "left",
        )
        .withColumn(
            "is_cpp_data_available",
            F.when(
                F.col("cpp_record_datetime").isNotNull()
                & F.col("cpp_equp_id").isNotNull(),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .drop("cpp_record_datetime", "cpp_equp_id")
    )

    # Join cpp data to ems data
    master_cpp_ems_join = (
        master_cpp_join.join(
            ems_df,
            (
                (
                    F.to_timestamp(
                        F.date_format(
                            master_cpp_join.master_record_datetime, "yyyy-MM-dd HH"
                        )
                    )
                    == ems_df.ems_record_datetime
                )
                & (master_df.master_equp_id == ems_df.ems_equp_id)
            ),
            "left",
        )
        .withColumn(
            "is_ems_data_available",
            F.when(
                F.col("ems_record_datetime").isNotNull()
                & F.col("ems_equp_id").isNotNull(),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .drop("ems_record_datetime", "ems_equp_id")
    )

    return master_cpp_ems_join
