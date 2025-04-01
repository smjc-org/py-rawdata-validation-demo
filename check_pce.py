import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V3"
formoid = "PCE"
formnm = "器械使用便捷性评价"

create_query_pce = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_pce_missing(df_pce: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_pce, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_pce_pcedat_missing = df_pce[df_pce["PCEDAT"].isna()]
    if not df_pce_pcedat_missing.empty:
        df_query_list.append(create_query_pce(variable="PCEDAT", label="评价日期", subjid=df_pce_pcedat_missing["SUBJID"], query="此字段必填"))

    df_pce_pcespid_missing = df_pce[df_pce["PCESPID"].isna()]
    if not df_pce_pcespid_missing.empty:
        df_query_list.append(create_query_pce(variable="PCESPID", label="记录号", subjid=df_pce_pcespid_missing["SUBJID"], query="此字段必填"))

    df_pce_pceterm_missing = df_pce[df_pce["PCETERM"].isna()]
    if not df_pce_pceterm_missing.empty:
        df_query_list.append(create_query_pce(variable="PCETERM", label="评估项目", subjid=df_pce_pceterm_missing["SUBJID"], query="此字段必填"))

    df_pce_pceorres_missing = df_pce[df_pce["PCEORRES"].isna()]
    if not df_pce_pceorres_missing.empty:
        df_query_list.append(create_query_pce(variable="PCEORRES", label="评估结果", subjid=df_pce_pceorres_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_pce_logic(df_pce: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_pce = df_pce.loc[:, ["SUBJID", "PCEDAT"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]

    # 【评价日期】不在CT检查完成后3天内，请核实
    df = df_pce.merge(df_ct, on="SUBJID", how="left")
    df_pce_logic = df.groupby("SUBJID").filter(lambda x: any((x["PCEDAT"] < x["CTDAT"]) | (x["PCEDAT"] > x["CTDAT"] + pd.Timedelta(days=3)))).reset_index()
    if not df_pce_logic.empty:
        df_query_list.append(create_query_pce(variable="PCEDAT", label="评价日期", subjid=df_pce_logic["SUBJID"], query="【评价日期】不在CT检查完成后3天内，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_pce(df_pce: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_pce = df_pce.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_pce_missing(df_pce))
    df_query_list.append(check_pce_logic(df_pce, df_ct))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
