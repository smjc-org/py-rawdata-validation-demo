import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "COMPAGE"
formoid = "CN"
formnm = "伴随治疗"

create_query_cn = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_cn_missing(df_cn: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_cn, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_cn_cnyn_missing = df_cn[df_cn["CNYN"].isna()]
    if not df_cn_cnyn_missing.empty:
        df_query_list.append(create_query_cn(variable="CNYN", label="有无伴随治疗", subjid=df_cn_cnyn_missing["SUBJID"], query="此字段必填"))

    df_cn_cnno_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNNO"].isna()]
    if not df_cn_cnno_missing.empty:
        df_query_list.append(create_query_cn(variable="CNNO", label="编号", subjid=df_cn_cnno_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnterm_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNTERM"].isna()]
    if not df_cn_cnterm_missing.empty:
        df_query_list.append(create_query_cn(variable="CNTERM", label="治疗名称", subjid=df_cn_cnterm_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnmeth_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNMETH"].isna()]
    if not df_cn_cnmeth_missing.empty:
        df_query_list.append(create_query_cn(variable="CNMETH", label="治疗方式", subjid=df_cn_cnmeth_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnfreq_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNFREQ"].isna()]
    if not df_cn_cnfreq_missing.empty:
        df_query_list.append(create_query_cn(variable="CNFREQ", label="治疗频率", subjid=df_cn_cnfreq_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnreas_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNREAS"].isna()]
    if not df_cn_cnreas_missing.empty:
        df_query_list.append(create_query_cn(variable="CNREAS", label="治疗原因", subjid=df_cn_cnreas_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnstdat_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNSTDAT"].isna()]
    if not df_cn_cnstdat_missing.empty:
        df_query_list.append(create_query_cn(variable="CNSTDAT", label="开始日期", subjid=df_cn_cnstdat_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnongo_missing = df_cn[(df_cn["CNYN"] == "有") & df_cn["CNONGO"].isna()]
    if not df_cn_cnongo_missing.empty:
        df_query_list.append(create_query_cn(variable="CNONGO", label="是否持续", subjid=df_cn_cnongo_missing["SUBJID"], query="【有无伴随治疗】选择“有”时，此字段必填"))

    df_cn_cnendat_missing = df_cn[(df_cn["CNONGO"] == "否") & df_cn["CNENDAT"].isna()]
    if not df_cn_cnendat_missing.empty:
        df_query_list.append(create_query_cn(variable="CNENDAT", label="结束日期", subjid=df_cn_cnendat_missing["SUBJID"], query="【是否持续】选择“否”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_cn_logic(df_cn: pd.DataFrame, df_info: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_cn = df_cn.loc[:, ["SUBJID", "CNSTDAT", "CNONGO", "CNENDAT"]]
    df_info = df_info.loc[:, ["SUBJID", "ICFDAT"]]

    # 【是否持续】选择“是”，【结束日期】不为空，请核实
    df_cn_logic = df_cn[(df_cn["CNONGO"] == "是") & df_cn["CNENDAT"].notna()]
    if not df_cn_logic.empty:
        df_query_list.append(create_query_cn(variable="CNENDAT", label="结束日期", subjid=df_cn_logic["SUBJID"], query="【是否持续】选择“是”，【结束日期】不为空，请核实"))

    # 【结束日期】早于【开始日期】，请核实
    df_cn_logic = df_cn[df_cn["CNENDAT"] < df_cn["CNSTDAT"]]
    if not df_cn_logic.empty:
        df_query_list.append(create_query_cn(variable="CNENDAT", label="结束日期", subjid=df_cn_logic["SUBJID"], query="【结束日期】早于【开始日期】，请核实"))

    # 【结束日期】早于【知情同意签署日期】，请核实
    df = df_cn.merge(df_info, on="SUBJID", how="left")
    df_cn_logic = df[df["CNENDAT"] < df["ICFDAT"]]
    if not df_cn_logic.empty:
        df_query_list.append(create_query_cn(variable="CNENDAT", label="结束日期", subjid=df_cn_logic["SUBJID"], query="【结束日期】早于【知情同意签署日期】，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_cn(df_cn: pd.DataFrame, df_info: pd.DataFrame) -> pd.DataFrame:
    df_cn = df_cn[df_cn["CNYN"] == "有"]
    if df_cn.empty:
        return pd.DataFrame()

    df_query_list = []
    df_query_list.append(check_cn_missing(df_cn))
    df_query_list.append(check_cn_logic(df_cn, df_info))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
