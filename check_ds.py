import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "COMPAGE"
formoid = "DS"
formnm = "试验总结"

create_query_ds = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ds_missing(df_ds: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ds, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ds_dsyn_missing = df_ds[df_ds["DSYN"].isna()]
    if not df_ds_dsyn_missing.empty:
        df_query_list.append(create_query_ds(variable="DSYN", label="是否完成整个临床试验", subjid=df_ds_dsyn_missing["SUBJID"], query="此字段必填"))

    df_ds_dscmpdat_missing = df_ds[(df_ds["DSYN"] == "是") & df_ds["DSCMPDAT"].isna()]
    if not df_ds_dscmpdat_missing.empty:
        df_query_list.append(create_query_ds(variable="DSCMPDAT", label="完成日期", subjid=df_ds_dscmpdat_missing["SUBJID"], query="【是否完成整个临床试验】选择“是”时，此字段必填"))

    df_ds_dsendat_missing = df_ds[(df_ds["DSYN"] == "否") & df_ds["DSENDAT"].isna()]
    if not df_ds_dsendat_missing.empty:
        df_query_list.append(create_query_ds(variable="DSENDAT", label="中止日期", subjid=df_ds_dsendat_missing["SUBJID"], query="【是否完成整个临床试验】选择“否”时，此字段必填"))

    df_ds_dsreas_missing = df_ds[(df_ds["DSYN"] == "否") & df_ds["DSREAS"].isna()]
    if not df_ds_dsreas_missing.empty:
        df_query_list.append(create_query_ds(variable="DSREAS", label="如提前退出试验，请选择最主要的原因（单选）", subjid=df_ds_dsreas_missing["SUBJID"], query="【是否完成整个临床试验】选择“否”时，此字段必填"))

    df_ds_dsreaso_missing = df_ds[(df_ds["DSREAS"] == "其他原因") & df_ds["DSREASO"].isna()]
    if not df_ds_dsreaso_missing.empty:
        df_query_list.append(create_query_ds(variable="DSREASO", label="其他原因", subjid=df_ds_dsreaso_missing["SUBJID"], query="【如提前退出试验，请选择最主要的原因（单选）】选择“其他原因”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ds_logic(df_ds: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ds(df_ds: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_query_list.append(check_ds_missing(df_ds))
    df_query_list.append(check_ds_logic(df_ds))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
