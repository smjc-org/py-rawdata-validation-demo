import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V1"
formoid = "IN"
formnm = "入选标准"

create_query_in = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_in_missing(df_in: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_in, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_in_inspid_missing = df_in[df_in["INSPID"].isna()]
    if not df_in_inspid_missing.empty:
        df_query_list.append(create_query_in(variable="INSPID", label="记录号", subjid=df_in_inspid_missing["SUBJID"], query="此字段必填"))

    df_in_interm_missing = df_in[df_in["INTERM"].isna()]
    if not df_in_interm_missing.empty:
        df_query_list.append(create_query_in(variable="INTERM", label="入选标准描述", subjid=df_in_interm_missing["SUBJID"], query="此字段必填"))

    df_in_inyn_missing = df_in[df_in["INYN"].isna()]
    if not df_in_inyn_missing.empty:
        df_query_list.append(create_query_in(variable="INYN", label="结果", subjid=df_in_inyn_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_in_logic(df_in: pd.DataFrame, df_info: pd.DataFrame, df_lb: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_in = df_in.loc[:, ["SUBJID", "INSPID", "INYN"]]
    df_info = df_info.loc[:, ["SUBJID", "SEX", "AGE"]]
    df_lb = df_lb.loc[:, ["SUBJID", "LBTERM", "LBORRES"]]

    # 18≤【年龄】≤75，入选标准1【18≤年龄≤75，性别不限】选择“否”，请核实
    df = df_in.merge(df_info, on="SUBJID", how="left")
    df_in_logic = df[(df["AGE"] >= 18) & (df["AGE"] <= 75) & (df["INSPID"] == 1) & (df["INYN"] == "否")]
    if not df_in_logic.empty:
        df_query_list.append(create_query_in(variable="INYN", label="结果", subjid=df_in_logic["SUBJID"], query="18≤【年龄】≤75，入选标准1【18≤年龄≤75，性别不限】选择“否”，请核实"))

    # 【年龄】＜18或【年龄】＞75，入选标准1【18≤年龄≤75，性别不限】选择“是”，请核实
    df = df_in.merge(df_info, on="SUBJID", how="left")
    df_in_logic = df[((df["AGE"] < 18) | (df["AGE"] > 75)) & (df["INSPID"] == 1) & (df["INYN"] == "是")]
    if not df_in_logic.empty:
        df_query_list.append(create_query_in(variable="INYN", label="结果", subjid=df_in_logic["SUBJID"], query="【年龄】＜18或【年龄】＞75，入选标准1【18≤年龄≤75，性别不限】选择“是”，请核实"))

    # 血妊娠【检查结果】为“阳性”，入选标准2【育龄期女性妊娠试验结果为阴性】选择“是”，请核实
    df = df_in.merge(df_lb, on="SUBJID", how="left")
    df_in_logic = df[(df["LBTERM"] == "血妊娠") & (df["LBORRES"] == "阳性") & (df["INSPID"] == 2) & (df["INYN"] == "是")]
    if not df_in_logic.empty:
        df_query_list.append(create_query_in(variable="INYN", label="结果", subjid=df_in_logic["SUBJID"], query="血妊娠【检查结果】为“阳性”，入选标准2【育龄期女性妊娠试验结果为阴性】选择“是”，请核实"))

    # 血妊娠【检查结果】为“阴性”，入选标准2【育龄期女性妊娠试验结果为阴性】选择“否”，请核实
    df = df_in.merge(df_lb, on="SUBJID", how="left")
    df_in_logic = df[(df["LBTERM"] == "血妊娠") & (df["LBORRES"] == "阴性") & (df["INSPID"] == 2) & (df["INYN"] == "否")]
    if not df_in_logic.empty:
        df_query_list.append(create_query_in(variable="INYN", label="结果", subjid=df_in_logic["SUBJID"], query="血妊娠【检查结果】为“阴性”，入选标准2【育龄期女性妊娠试验结果为阴性】选择“否”，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_in(df_in: pd.DataFrame, df_info: pd.DataFrame, df_lb: pd.DataFrame) -> pd.DataFrame:
    df_in = df_in.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_in_missing(df_in))
    df_query_list.append(check_in_logic(df_in, df_info, df_lb))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
