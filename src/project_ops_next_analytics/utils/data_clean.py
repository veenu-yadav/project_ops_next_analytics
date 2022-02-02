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
"""Common functions for DE part of pipeline"""
import re
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import sql
from typing import List
from pyspark.sql import DataFrame
import pandas as pd


def clean_pandas_date_format(pandas_df, col_list: list):
    """
    This function format date columns in pandas data frame.
    Args:
        pandas_df: pandas data frame.
        col_list: date column to be formatted.

    Returns: Returns pandas data frame with formatted date columns.

    """
    for col in col_list:
        pandas_df[col] = pandas_df[col].astype(str)
        pandas_df[col] = pandas_df[col].str.replace(".", "-")
        pandas_df[col] = pandas_df[col].apply(lambda x: pd.to_datetime(x))
    return pandas_df


def clean_column_name(column_name):
    """
    Refactor pandas column names including lower casing,
    and replacement of non-alpha characters with underscores or words.

    Args:
        column_name(str): a 'dirty' column name
    Returns:
        column_name(str): a 'clean' column name
    """
    column_new = column_name.lower().strip()
    column_new = re.sub(r"[ :_\-.]+", "_", column_new)
    column_new = re.sub(r"#", "number", column_new)
    column_new = re.sub(r"%", "percent", column_new)
    column_new = re.sub(r"[&+]+", "and", column_new)
    column_new = re.sub(r"[|,/;]+", "or", column_new)
    column_new = re.sub(r"[()?]+", "", column_new)
    column_new = re.sub(r" ", "_", column_new)
    column_new = re.sub(r"S+", "_", column_new)
    column_new = re.sub(r"=", "_", column_new)
    column_new = re.sub("[^A-Za-z0-9]+", "_", column_new)
    column_new = re.sub(r"(_$)", "", column_new)  # removes _ at the end of column name
    column_new = re.sub(
        r"(^_)", "", column_new
    )  # removes _ at the beginning if added during above steps

    return column_new


def clean_spark_column_names(spark_df):
    """
    Refactor spark column names including lower casing,
    and replacement of non-alpha characters with underscores or words.

    Args:
        spark_df(pyspark.sql.dataframe.DataFrame): a dataframe
    Returns:
        spark_df(pyspark.sql.dataframe.DataFrame): dataframe with column names in lowercase and non-alpha characters
        substituted
    """

    return spark_df.toDF(*[clean_column_name(column) for column in spark_df.columns])


def clean_pandas_column_names(pandas_df):
    """
    Refactor pandas column names including lower casing,
    and replacement of non-alpha characters with underscores or words.

    Args:
        pandas_df(pandas.DataFrame): a dataframe
    Returns:
        pandas_df(pandas.DataFrame): dataframe with column names in lowercase and non-alpha characters substituted
    """

    column_dict = {}
    for i, column in enumerate(pandas_df):
        column_dict[column] = clean_column_name(column)

    return pandas_df.rename(columns=column_dict)


def trim_all_columns(df):
    """
    Trim values of all columns

    Args:
        df(pyspark.sql.dataframe.DataFrame): input dataframe
    Returns:
        df(pyspark.sql.dataframe.DataFrame): dataframe with values trimmed of whitespaces
    """

    trim_columns = [
        F.trim(F.col(column)).alias(column) if type == "string" else column
        for column, type in iter(df.dtypes)
    ]
    return df.select(trim_columns)


def remove_duplicates(
    df: sql.DataFrame, group_cols: List[str] = None, dedupe_cols: List[str] = None
) -> sql.DataFrame:
    """
    Function removes unnecessary duplication in a dataframe, take last record based on timestamp

    Args:
        df: sql dataframe that needs to be deduplicated
        group_cols: group columns which should be unique
        dedupe_date_cols: columns that need to be deduplicated

    Returns:
        deduplicated DF

    """
    if (group_cols == None) & (dedupe_cols == None):
        df = df.drop_duplicates()
        return df

    else:
        dedup_window = Window.partitionBy(*group_cols).orderBy(
            F.col(*dedupe_cols).desc()
        )
        return (
            df.withColumn("is_latest", F.row_number().over(dedup_window))
            .filter("is_latest == 1")
            .drop("is_latest")
        )


def remove_pattern_from_column(spark_df: DataFrame, pattern) -> sql.DataFrame:
    """
    This function removes a given pattern from spark data frame column names.
    Args:
        spark_df: spark data frame
        pattern: pattern from column names to be removed

    Returns: Returns a given pattern from spark data frame column names.

    """
    return spark_df.toDF(*[re.sub(pattern, "", column) for column in spark_df.columns])


def clean_csvdata(spark_dataset: DataFrame):
    """
    The function cleans a csv spark data frame.
    Args:
        spark_dataset: spark data frame

    Returns: Returns cleaned spark data frame.

    """
    data = clean_spark_column_names(spark_dataset)
    data_sdf = trim_all_columns(data)
    data_sdf = remove_duplicates(data_sdf)
    return data_sdf


def remove_special_character(df: sql.DataFrame, colname: List) -> sql.DataFrame:
    """
    This function removes special character from a column in spark data frame.
    Args:
        df: spark data frame
        colname: the column from which special characters need to be removed.

    Returns: Cleaned spark data frame with special character from given column removed.

    """
    for column in colname:
        df = df.withColumn(column, F.regexp_replace(df[column], "\p{C}+", "_"))
    return df


def remove_brackets_character(df: sql.DataFrame, colname: List) -> sql.DataFrame:
    """
    This function removes bracket character from a column in spark data frame.
    Args:
        df: spark data frame
        colname: the column from which bracket characters need to be removed.

    Returns: Cleaned spark data frame with bracket character from given column removed.

    """
    for column in colname:
        df = df.withColumn(
            column, F.regexp_replace(df[column], "\(|\)|\{|\}|\[|\]", "_")
        )
    return df


def remove_spaces_character(df: sql.DataFrame, colname: List) -> sql.DataFrame:
    """
    This function removes spaces character from a column in spark data frame.
    Args:
        df: spark data frame
        colname: the column from which spaces characters need to be removed.

    Returns: Cleaned spark data frame with spaces character from given column removed.

    """
    for column in colname:
        df = df.withColumn(column, F.regexp_replace(df[column], " ", "_"))
    return df


def remove_extra_character(df: sql.DataFrame, colname: List) -> sql.DataFrame:
    """
    This function removes back slash character from a column in spark data frame.
    Args:
        df: spark data frame
        colname: the column from which back slash characters need to be removed.

    Returns: Cleaned spark data frame with back slash character from given column removed.

    """
    for column in colname:
        df = df.withColumn(column, F.regexp_replace(df[column], "/", "_"))
        # .withColumn(column, F.regexp_replace(df[column], "\\", "_"))
    return df


def lower_case(df: sql.DataFrame, colname: List) -> sql.DataFrame:
    """
    This function converts a column into lower case.
    Args:
        df: spark data set
        colname: the column which needs to be converted into lower case

    Returns: The data set with given column name into lower case.

    """
    for column in colname:
        df = df.withColumn(column, F.lower(df[column]))
    return df


def create_substring(
    df: sql.DataFrame, input_colname, output_colname, start_pos, length
) -> sql.DataFrame:
    """
    This function returns a spark data frame with new column created by applying substring operation on the existing
    column.
    Args:
        df: spark data frame
        input_colname: input column on which sub string column need to be applied
        output_colname: output column generated by applying sub string operation on the input column
        start_pos: start position for selecting sub string
        length: the length of sub string

    Returns: Returns a spark data frame with new column created by applying substring operation on the existing
    column.
    column.

    """
    df = df.withColumn(output_colname, df[input_colname].substr(start_pos, length))
    return df


def add_prefix_to_colname(df, prefix, exceptions):
    """
    This function returns a spark data frame with prefix added to all the column names except for the columns given
    in exceptions list.
    Args:
        df: spark data frame
        prefix: the prefix to be added to spark data frame columns
        exceptions: the column names where prefix doesn't need to be added

    Returns: Returns spark data frame with prefix added to all the column names except for the columns given
    in exceptions list.

    """
    for c in df.columns:
        if c not in exceptions:
            df = df.withColumnRenamed(c, "{}{}".format(prefix, c))

    return df


def format_col_values(df: sql.DataFrame, colname: List) -> sql.DataFrame:
    """
    This function removes special character, bracket character, spaces character, backslash character from a column
    in spark data frame.
    It converts the given column name into lower case.
    Args:
        df: spark data frame
        colname: the column from which back slash characters need to be removed.

    Returns: Cleaned spark data frame with special character, bracket character, spaces character, backslash character
     from given column removed along with the given column name is converted into lower case.

    """
    df = remove_special_character(df, colname)
    df = remove_brackets_character(df, colname)
    df = remove_spaces_character(df, colname)
    df = remove_extra_character(df, colname)
    df = lower_case(df, colname)
    return df
