import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V3"
formoid = "PSE"
formnm = "整机功能性及稳定性满意度评价"

create_query_pse = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_pse_missing(df_pse: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_pse, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_pse_psedat_missing = df_pse[df_pse["PSEDAT"].isna()]
    if not df_pse_psedat_missing.empty:
        df_query_list.append(create_query_pse(variable="PSEDAT", label="评价日期", subjid=df_pse_psedat_missing["SUBJID"], query="此字段必填"))

    df_pse_psespid_missing = df_pse[df_pse["PSESPID"].isna()]
    if not df_pse_psespid_missing.empty:
        df_query_list.append(create_query_pse(variable="PSESPID", label="记录号", subjid=df_pse_psespid_missing["SUBJID"], query="此字段必填"))

    df_pse_pseterm_missing = df_pse[df_pse["PSETERM"].isna()]
    if not df_pse_pseterm_missing.empty:
        df_query_list.append(create_query_pse(variable="PSETERM", label="评估项目", subjid=df_pse_pseterm_missing["SUBJID"], query="此字段必填"))

    df_pse_pseorres_missing = df_pse[df_pse["PSEORRES"].isna()]
    if not df_pse_pseorres_missing.empty:
        df_query_list.append(create_query_pse(variable="PSEORRES", label="评估结果", subjid=df_pse_pseorres_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_pse_logic(df_pse: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_pse = df_pse.loc[:, ["SUBJID", "PSEDAT"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]

    # 【评价日期】不在CT检查完成后3天内，请核实
    df = df_pse.merge(df_ct, on="SUBJID", how="left")
    df_pse_logic = df.groupby("SUBJID").filter(lambda x: any((x["PSEDAT"] < x["CTDAT"]) | (x["PSEDAT"] > x["CTDAT"] + pd.Timedelta(days=3)))).reset_index()
    if not df_pse_logic.empty:
        df_query_list.append(create_query_pse(variable="PSEDAT", label="评价日期", subjid=df_pse_logic["SUBJID"], query="【评价日期】不在CT检查完成后3天内，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_pse(df_pse: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_pse = df_pse.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_pse_missing(df_pse))
    df_query_list.append(check_pse_logic(df_pse, df_ct))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
