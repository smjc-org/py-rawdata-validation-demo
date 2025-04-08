import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V3"
formoid = "IQE"
formnm = "图像质量评价"

create_query_iqe = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_iqe_missing(df_iqe: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_iqe, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_iqe_iqespid_missing = df_iqe[df_iqe["IQESPID"].isna()]
    if not df_iqe_iqespid_missing.empty:
        df_query_list.append(create_query_iqe(variable="IQESPID", label="评价者", subjid=df_iqe_iqespid_missing["SUBJID"], query="此字段必填"))

    df_iqe_iqeterm_missing = df_iqe[df_iqe["IQETERM"].isna()]
    if not df_iqe_iqeterm_missing.empty:
        df_query_list.append(create_query_iqe(variable="IQETERM", label="显示器分辨率", subjid=df_iqe_iqeterm_missing["SUBJID"], query="此字段必填"))

    df_iqe_iqeres_missing = df_iqe[df_iqe["IQERES"].isna()]
    if not df_iqe_iqeres_missing.empty:
        df_query_list.append(create_query_iqe(variable="IQERES", label="整体评分", subjid=df_iqe_iqeres_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_iqe_logic(df_iqe: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_iqe(df_iqe: pd.DataFrame) -> pd.DataFrame:
    df_iqe = df_iqe.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_iqe_missing(df_iqe))
    df_query_list.append(check_iqe_logic(df_iqe))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
