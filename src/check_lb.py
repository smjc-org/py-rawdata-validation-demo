import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V1"
formoid = "LB"
formnm = "实验室检查"

create_query_lb = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_lb_missing(df_lb: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_lb, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_lb_lbterm_missing = df_lb[df_lb["LBTERM"].isna()]
    if not df_lb_lbterm_missing.empty:
        df_query_list.append(create_query_lb(variable="LBTERM", label="检查项目", subjid=df_lb_lbterm_missing["SUBJID"], query="此字段必填"))

    df_lb_lbperf_missing = df_lb[df_lb["LBPERF"].isna()]
    if not df_lb_lbperf_missing.empty:
        df_query_list.append(create_query_lb(variable="LBPERF", label="是否进行检查完成", subjid=df_lb_lbperf_missing["SUBJID"], query="此字段必填"))

    df_lb_lbreas_missing = df_lb[(df_lb["LBPERF"] == "否") & (df_lb["LBREAS"].isna())]
    if not df_lb_lbreas_missing.empty:
        df_query_list.append(create_query_lb(variable="LBREAS", label="未查原因", subjid=df_lb_lbreas_missing["SUBJID"], query="【是否进行检查完成】选择“否”时，此字段必填"))

    df_lb_lborres_missing = df_lb[(df_lb["LBPERF"] == "是") & (df_lb["LBORRES"].isna())]
    if not df_lb_lborres_missing.empty:
        df_query_list.append(create_query_lb(variable="LBORRES", label="检查结果", subjid=df_lb_lborres_missing["SUBJID"], query="【是否进行检查完成】选择“是”时，此字段必填"))

    df_lb_lbornrlo_missing = df_lb[(df_lb["LBPERF"] == "是") & (df_lb["LBORNRLO"].isna())]
    if not df_lb_lbornrlo_missing.empty:
        df_query_list.append(create_query_lb(variable="LBORNRLO", label="下限", subjid=df_lb_lbornrlo_missing["SUBJID"], query="【是否进行检查完成】选择“是”时，此字段必填"))

    df_lb_lbornrhi_missing = df_lb[(df_lb["LBPERF"] == "是") & (df_lb["LBORNRHI"].isna())]
    if not df_lb_lbornrhi_missing.empty:
        df_query_list.append(create_query_lb(variable="LBORNRHI", label="上限", subjid=df_lb_lbornrhi_missing["SUBJID"], query="【是否进行检查完成】选择“是”时，此字段必填"))

    df_lb_lbclsig_missing = df_lb[(df_lb["LBPERF"] == "是") & (df_lb["LBCLSIG"].isna())]
    if not df_lb_lbclsig_missing.empty:
        df_query_list.append(create_query_lb(variable="LBCLSIG", label="临床意义判定", subjid=df_lb_lbclsig_missing["SUBJID"], query="【是否进行检查完成】选择“是”时，此字段必填"))

    df_lb_lbresoth_missing = df_lb[(df_lb["LBCLSIG"] == "异常有临床意义") & (df_lb["LBRESOTH"].isna())]
    if not df_lb_lbresoth_missing.empty:
        df_query_list.append(create_query_lb(variable="LBRESOTH", label="备注", subjid=df_lb_lbresoth_missing["SUBJID"], query="【临床意义判定】选择“异常有临床意义”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_lb_logic(df_lb: pd.DataFrame, df_info: pd.DataFrame, df_ct: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_lb = df_lb.loc[:, ["SUBJID", "LBPERF", "LBDAT", "LBTERM", "LBORRES", "LBCLSIG"]]
    df_info = df_info.loc[:, ["SUBJID", "SEX", "AGE"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]
    df_ie = df_ie.loc[:, ["SUBJID", "ECTCYN"]]

    # 【性别】为“女”或【初拟增强检查经筛选后是否有变更】选择“无”，此字段必填
    df = df_lb.merge(df_info, on="SUBJID", how="left").merge(df_ie, on="SUBJID", how="left")
    df_lb_logic = df[((df["SEX"] == "女") | (df["ECTCYN"] == "无")) & df["LBDAT"].isna()]
    if not df_lb_logic.empty:
        df_query_list.append(create_query_lb(variable="LBDAT", label="采样日期", subjid=df_lb_logic["SUBJID"], query="【性别】为“女”或【初拟增强检查经筛选后是否有变更】选择“无”，此字段必填"))

    # 【采样日期】不在CT检查前三天内或CT检查当天，请核实
    df = df_lb.merge(df_ct, on="SUBJID", how="left")
    df_days_diff = (df["LBDAT"] - df["CTDAT"]).dt.days
    df_lb_logic = df[(df_days_diff < -3) | (df_days_diff > 0)]
    if not df_lb_logic.empty:
        df_query_list.append(create_query_lb(variable="CTDAT", label="采样日期", subjid=df_lb_logic["SUBJID"], query="【采样日期】不在CT检查前三天内或CT检查当天，请核实"))

    # 【是否进行检查完成】选择“否”或不适用，【检查结果】不为空，请核实
    df_lb_logic = df_lb[((df_lb["LBPERF"] == "否") | (df_lb["LBPERF"] == "不适用")) & (df_lb["LBORRES"].notna())]
    if not df_lb_logic.empty:
        df_query_list.append(create_query_lb(variable="LBORRES", label="检查结果", subjid=df_lb_logic["SUBJID"], query="【是否进行检查完成】选择“否”或不适用，【检查结果】不为空，请核实"))

    # 【是否进行检查完成】选择“否”或不适用，【临床意义判定】不为空，请核实
    df_lb_logic = df_lb[((df_lb["LBPERF"] == "否") | (df_lb["LBPERF"] == "不适用")) & (df_lb["LBCLSIG"].notna())]
    if not df_lb_logic.empty:
        df_query_list.append(create_query_lb(variable="LBCLSIG", label="临床意义判定", subjid=df_lb_logic["SUBJID"], query="【是否进行检查完成】选择“否”或不适用，【临床意义判定】不为空，请核实"))

    # 【性别】为男，血妊娠【是否进行检查完成】未选择“不适用”，请核实
    df = df_lb.merge(df_info, on="SUBJID", how="left")
    df_lb_logic = df[(df["SEX"] == "男") & (df["LBTERM"] == "血妊娠") & (df["LBPERF"] != "不适用")]
    if not df_lb_logic.empty:
        df_query_list.append(create_query_lb(variable="LBPERF", label="是否进行检查完成", subjid=df_lb_logic["SUBJID"], query="【性别】为男，血妊娠【是否进行检查完成】未选择“不适用”，请核实"))

    # 【性别】为女且【年龄】在18-55岁，血妊娠【是否进行检查完成】未选择“是”，请核实
    df = df_lb.merge(df_info, on="SUBJID", how="left")
    df_lb_logic = df[(df["SEX"] == "女") & (df["AGE"] >= 18) & (df["AGE"] <= 55) & (df["LBTERM"] == "血妊娠") & (df["LBPERF"] != "是")]
    if not df_lb_logic.empty:
        df_query_list.append(create_query_lb(variable="LBPERF", label="是否进行检查完成", subjid=df_lb_logic["SUBJID"], query="【性别】为女且【年龄】在18-55岁，血妊娠【是否进行检查完成】未选择“是”，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_lb(df_lb: pd.DataFrame, df_info: pd.DataFrame, df_ct: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_query_list.append(check_lb_missing(df_lb))
    df_query_list.append(check_lb_logic(df_lb, df_info, df_ct, df_ie))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
