import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V1"
formoid = "INFO"
formnm = "基本信息"

create_query_info = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_info_missing(df_info: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_info, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_info_icfdat_missing = df_info[df_info["ICFDAT"].isna()]
    if not df_info_icfdat_missing.empty:
        df_query_list.append(create_query_info(variable="ICFDAT", label="知情同意签署日期", subjid=df_info_icfdat_missing["SUBJID"], query="此字段必填"))

    df_info_sex_missing = df_info[df_info["SEX"].isna()]
    if not df_info_sex_missing.empty:
        df_query_list.append(create_query_info(variable="SEX", label="性别", subjid=df_info_sex_missing["SUBJID"], query="此字段必填"))

    df_info_age_missing = df_info[df_info["AGE"].isna()]
    if not df_info_age_missing.empty:
        df_query_list.append(create_query_info(variable="AGE", label="年龄", subjid=df_info_age_missing["SUBJID"], query="此字段必填"))

    df_info_sbp_missing = df_info[df_info["SBP"].isna()]
    if not df_info_sbp_missing.empty:
        df_query_list.append(create_query_info(variable="SBP", label="收缩压", subjid=df_info_sbp_missing["SUBJID"], query="此字段必填"))

    df_info_dbp_missing = df_info[df_info["DBP"].isna()]
    if not df_info_dbp_missing.empty:
        df_query_list.append(create_query_info(variable="DBP", label="舒张压", subjid=df_info_dbp_missing["SUBJID"], query="此字段必填"))

    df_info_hr_missing = df_info[df_info["HR"].isna()]
    if not df_info_hr_missing.empty:
        df_query_list.append(create_query_info(variable="HR", label="心率", subjid=df_info_hr_missing["SUBJID"], query="此字段必填"))

    df_info_height_missing = df_info[df_info["HEIGHT"].isna()]
    if not df_info_height_missing.empty:
        df_query_list.append(create_query_info(variable="HEIGHT", label="身高", subjid=df_info_height_missing["SUBJID"], query="此字段必填"))

    df_info_weight_missing = df_info[df_info["WEIGHT"].isna()]
    if not df_info_weight_missing.empty:
        df_query_list.append(create_query_info(variable="WEIGHT", label="体重", subjid=df_info_weight_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_info_logic(df_info: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_info = df_info.loc[:, ["SUBJID", "ICFDAT"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]

    # 【知情同意签署日期】不在CT检查前3天内或CT检查当天，请核实
    df = df_info.merge(df_ct, on="SUBJID", how="left")
    df_days_diff = (df["ICFDAT"] - df["CTDAT"]).dt.days
    df_info_logic = df[(df_days_diff < -3) | (df_days_diff > 0)]
    if not df_info_logic.empty:
        df_query_list.append(create_query_info(variable="ICFDAT", label="知情同意签署日期", subjid=df_info_logic["SUBJID"], query="【知情同意签署日期】不在CT检查前3天内或CT检查当天，请核实"))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_info(df_info: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_query_list.append(check_info_missing(df_info))
    df_query_list.append(check_info_logic(df_info, df_ct))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
