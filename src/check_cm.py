import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "COMPAGE"
formoid = "CM"
formnm = "合并用药"

create_query_cm = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_cm_missing(df_cm: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_cm, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_cm_cmyn_missing = df_cm[df_cm["CMYN"].isna()]
    if not df_cm_cmyn_missing.empty:
        df_query_list.append(create_query_cm(variable="CMYN", label="有无合并用药", subjid=df_cm_cmyn_missing["SUBJID"], query="此字段必填"))

    df_cm_cmno_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMNO"].isna()]
    if not df_cm_cmno_missing.empty:
        df_query_list.append(create_query_cm(variable="CMNO", label="编号", subjid=df_cm_cmno_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmterm_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMTERM"].isna()]
    if not df_cm_cmterm_missing.empty:
        df_query_list.append(create_query_cm(variable="CMTERM", label="药物名称", subjid=df_cm_cmterm_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmdose_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMDOSE"].isna()]
    if not df_cm_cmdose_missing.empty:
        df_query_list.append(create_query_cm(variable="CMDOSE", label="药物剂量", subjid=df_cm_cmdose_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmdoseu_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMDOSEU"].isna()]
    if not df_cm_cmdoseu_missing.empty:
        df_query_list.append(create_query_cm(variable="CMDOSEU", label="剂量单位", subjid=df_cm_cmdoseu_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmfreq_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMFREQ"].isna()]
    if not df_cm_cmfreq_missing.empty:
        df_query_list.append(create_query_cm(variable="CMFREQ", label="给药频率", subjid=df_cm_cmfreq_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmroute_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMROUTE"].isna()]
    if not df_cm_cmroute_missing.empty:
        df_query_list.append(create_query_cm(variable="CMROUTE", label="给药途径", subjid=df_cm_cmroute_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmreas_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMREAS"].isna()]
    if not df_cm_cmreas_missing.empty:
        df_query_list.append(create_query_cm(variable="CMREAS", label="给药原因", subjid=df_cm_cmreas_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmstdat_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMSTDAT"].isna()]
    if not df_cm_cmstdat_missing.empty:
        df_query_list.append(create_query_cm(variable="CMSTDAT", label="开始日期", subjid=df_cm_cmstdat_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmongo_missing = df_cm[(df_cm["CMYN"] == "有") & df_cm["CMONGO"].isna()]
    if not df_cm_cmongo_missing.empty:
        df_query_list.append(create_query_cm(variable="CMONGO", label="是否持续", subjid=df_cm_cmongo_missing["SUBJID"], query="【有无合并用药】选择“有”时，此字段必填"))

    df_cm_cmendat_missing = df_cm[(df_cm["CMONGO"] == "否") & df_cm["CMENDAT"].isna()]
    if not df_cm_cmendat_missing.empty:
        df_query_list.append(create_query_cm(variable="CMENDAT", label="结束日期", subjid=df_cm_cmendat_missing["SUBJID"], query="【是否持续】选择“否”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_cm_logic(df_cm: pd.DataFrame, df_ae: pd.DataFrame, df_info: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_cm = df_cm.loc[:, ["SUBJID", "CMYN", "CMSTDAT", "CMONGO", "CMENDAT"]]
    df_ae = df_ae.loc[:, ["SUBJID", "AEACN"]]
    df_info = df_info.loc[:, ["SUBJID", "ICFDAT"]]

    # 不良事件【对不良事件采取的措施】选择“药物治疗”，【有无合并用药】选择“无”，请核实
    df_ae_aeacn_drug_subjid = df_ae[df_ae["AEACN"] == "药物治疗"]["SUBJID"].unique()
    df = df_cm[df_cm["SUBJID"].isin(df_ae_aeacn_drug_subjid)]
    df_cm_logic = df.groupby("SUBJID").filter(lambda x: all(x["CMYN"] == "无")).reset_index()
    if not df_cm_logic.empty:
        df_query_list.append(create_query_cm(variable="CMYN", label="有无合并用药", subjid=df_cm_logic["SUBJID"], query="不良事件【对不良事件采取的措施】选择“药物治疗”，【有无合并用药】选择“无”，请核实"))

    # 【是否持续】选择“是”，【结束日期】不为空，请核实
    df_cm_logic = df_cm[(df_cm["CMONGO"] == "是") & df_cm["CMENDAT"].notna()]
    if not df_cm_logic.empty:
        df_query_list.append(create_query_cm(variable="CMENDAT", label="结束日期", subjid=df_cm_logic["SUBJID"], query="【是否持续】选择“是”，【结束日期】不为空，请核实"))

    # 【结束日期】早于【开始日期】，请核实
    df_cm_logic = df_cm[df_cm["CMENDAT"] < df_cm["CMSTDAT"]]
    if not df_cm_logic.empty:
        df_query_list.append(create_query_cm(variable="CMENDAT", label="结束日期", subjid=df_cm_logic["SUBJID"], query="【结束日期】早于【开始日期】，请核实"))

    # 【结束日期】早于【知情同意签署日期】，请核实
    df = pd.merge(df_cm, df_info, on="SUBJID")
    df_cm_logic = df[df["CMENDAT"] < df["ICFDAT"]]
    if not df_cm_logic.empty:
        df_query_list.append(create_query_cm(variable="CMENDAT", label="结束日期", subjid=df_cm_logic["SUBJID"], query="【结束日期】早于【知情同意签署日期】，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_cm(df_cm: pd.DataFrame, df_ae: pd.DataFrame, df_info: pd.DataFrame) -> pd.DataFrame:
    df_cm = df_cm[df_cm["CMYN"] == "有"]
    if df_cm.empty:
        return pd.DataFrame()

    df_query_list = []
    df_query_list.append(check_cm_missing(df_cm))
    df_query_list.append(check_cm_logic(df_cm, df_ae, df_info))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
