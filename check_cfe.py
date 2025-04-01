import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V3"
formoid = "CFE"
formnm = "常用功能评价"

create_query_cfe = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_cfe_missing(df_cfe: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_cfe, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_cfe_cfedat_missing = df_cfe[df_cfe["CFEDAT"].isna()]
    if not df_cfe_cfedat_missing.empty:
        df_query_list.append(create_query_cfe(variable="CFEDAT", label="评价日期", subjid=df_cfe_cfedat_missing["SUBJID"], query="此字段必填"))

    df_cfe_cfespid_missing = df_cfe[df_cfe["CFESPID"].isna()]
    if not df_cfe_cfespid_missing.empty:
        df_query_list.append(create_query_cfe(variable="CFESPID", label="记录号", subjid=df_cfe_cfespid_missing["SUBJID"], query="此字段必填"))

    df_cfe_cfeterm_missing = df_cfe[df_cfe["CFETERM"].isna()]
    if not df_cfe_cfeterm_missing.empty:
        df_query_list.append(create_query_cfe(variable="CFETERM", label="评估项目", subjid=df_cfe_cfeterm_missing["SUBJID"], query="此字段必填"))

    df_cfe_cfeorres_missing = df_cfe[df_cfe["CFEORRES"].isna()]
    if not df_cfe_cfeorres_missing.empty:
        df_query_list.append(create_query_cfe(variable="CFEORRES", label="评估结果", subjid=df_cfe_cfeorres_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_cfe_logic(df_cfe: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_cfe = df_cfe.loc[:, ["SUBJID", "CFEDAT"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]

    # 【评价日期】不在CT检查完成后3天内，请核实
    df = df_cfe.merge(df_ct, on="SUBJID", how="left")
    df_cfe_logic = df.groupby("SUBJID").filter(lambda x: any((x["CFEDAT"] < x["CTDAT"]) | (x["CFEDAT"] > x["CTDAT"] + pd.Timedelta(days=3)))).reset_index()
    if not df_cfe_logic.empty:
        df_query_list.append(create_query_cfe(variable="CFEDAT", label="评价日期", subjid=df_cfe_logic["SUBJID"], query="【评价日期】不在CT检查完成后3天内，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_cfe(df_cfe: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_cfe = df_cfe.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_cfe_missing(df_cfe))
    df_query_list.append(check_cfe_logic(df_cfe, df_ct))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
