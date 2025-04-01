import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common


vistoid = "COMPAGE"
formoid = "PD"
formnm = "方案偏离记录"

create_query_pd = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_pd_missing(df_pd: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_pd, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_pd_pdyn_missing = df_pd[df_pd["PDYN"].isna()]
    if not df_pd_pdyn_missing.empty:
        df_query_list.append(create_query_pd(variable="PDYN", label="是否发生方案偏离", subjid=df_pd_pdyn_missing["SUBJID"], query="此字段必填"))

    df_pd_pdno_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDNO"].isna()]
    if not df_pd_pdno_missing.empty:
        df_query_list.append(create_query_pd(variable="PDNO", label="编号", subjid=df_pd_pdno_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    df_pd_pddesc_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDDESC"].isna()]
    if not df_pd_pddesc_missing.empty:
        df_query_list.append(create_query_pd(variable="PDDESC", label="方案偏离描述", subjid=df_pd_pddesc_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    df_pd_pdstdat_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDSTDAT"].isna()]
    if not df_pd_pdstdat_missing.empty:
        df_query_list.append(create_query_pd(variable="PDSTDAT", label="发生日期", subjid=df_pd_pdstdat_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    df_pd_pdreas_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDREAS"].isna()]
    if not df_pd_pdreas_missing.empty:
        df_query_list.append(create_query_pd(variable="PDREAS", label="发生原因", subjid=df_pd_pdreas_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    df_pd_pdsev_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDSEV"].isna()]
    if not df_pd_pdsev_missing.empty:
        df_query_list.append(create_query_pd(variable="PDSEV", label="严重程度", subjid=df_pd_pdsev_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    df_pd_pdacn_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDACN"].isna()]
    if not df_pd_pdacn_missing.empty:
        df_query_list.append(create_query_pd(variable="PDACN", label="纠正/预防措施", subjid=df_pd_pdacn_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    df_pd_pdorres_missing = df_pd[(df_pd["PDYN"] == "是") & df_pd["PDORRES"].isna()]
    if not df_pd_pdorres_missing.empty:
        df_query_list.append(create_query_pd(variable="PDORRES", label="结果", subjid=df_pd_pdorres_missing["SUBJID"], query="【是否发生方案偏离】选择“是”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_pd_logic(df_pd: pd.DataFrame, df_info: pd.DataFrame, df_ds: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_pd = df_pd.loc[:, ["SUBJID", "PDSTDAT"]]
    df_info = df_info.loc[:, ["SUBJID", "ICFDAT"]]
    df_ds = df_ds.loc[:, ["SUBJID", "DSYN", "DSCMPDAT", "DSENDAT"]]

    # 【发生日期】早于【知情同意签署日期】，请核实
    df = df_pd.merge(df_info, on="SUBJID", how="left")
    df_pd_logic = df[df["PDSTDAT"] < df["ICFDAT"]]
    if not df_pd_logic.empty:
        df_query_list.append(create_query_pd(variable="PDSTDAT", label="发生日期", subjid=df_pd_logic["SUBJID"], query="【发生日期】早于【知情同意签署日期】，请核实"))

    # 试验总结【是否完成整个临床试验】选择“是”，【发生日期】晚于试验总结【完成日期】，请核实
    df = df_pd.merge(df_ds, on="SUBJID", how="left")
    df_pd_logic = df[(df["DSYN"] == "是") & (df["PDSTDAT"] > df["DSCMPDAT"])]
    if not df_pd_logic.empty:
        df_query_list.append(create_query_pd(variable="PDSTDAT", label="发生日期", subjid=df_pd_logic["SUBJID"], query="试验总结【是否完成整个临床试验】选择“是”，【发生日期】晚于试验总结【完成日期】，请核实"))

    # 试验总结【是否完成整个临床试验】选择“否”，【发生日期】晚于试验总结【中止日期】，请核实
    df = df_pd.merge(df_ds, on="SUBJID", how="left")
    df_pd_logic = df[(df["DSYN"] == "否") & (df["PDSTDAT"] > df["DSENDAT"])]
    if not df_pd_logic.empty:
        df_query_list.append(create_query_pd(variable="PDSTDAT", label="发生日期", subjid=df_pd_logic["SUBJID"], query="试验总结【是否完成整个临床试验】选择“否”，【发生日期】晚于试验总结【中止日期】，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_pd(df_pd: pd.DataFrame, df_info: pd.DataFrame, df_ds: pd.DataFrame) -> pd.DataFrame:
    df_pd = df_pd[df_pd["PDYN"] == "是"]
    if df_pd.empty:
        return pd.DataFrame()

    df_query_list = []
    df_query_list.append(check_pd_missing(df_pd))
    df_query_list.append(check_pd_logic(df_pd, df_info, df_ds))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
