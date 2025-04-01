import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "COMPAGE"
formoid = "ED"
formnm = "器械缺陷记录"

create_query_ed = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ed_missing(df_ed: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ed, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ed_edyn_missing = df_ed[df_ed["EDYN"].isna()]
    if not df_ed_edyn_missing.empty:
        df_query_list.append(create_query_ed(variable="EDYN", label="是否发生器械缺陷", subjid=df_ed_edyn_missing["SUBJID"], query="此字段必填"))

    df_ed_edno_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDNO"].isna()]
    if not df_ed_edno_missing.empty:
        df_query_list.append(create_query_ed(variable="EDNO", label="编号", subjid=df_ed_edno_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    df_ed_eddesc_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDDESC"].isna()]
    if not df_ed_eddesc_missing.empty:
        df_query_list.append(create_query_ed(variable="EDDESC", label="缺陷描述", subjid=df_ed_eddesc_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    df_ed_edcat_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDCAT"].isna()]
    if not df_ed_edcat_missing.empty:
        df_query_list.append(create_query_ed(variable="EDCAT", label="缺陷分类", subjid=df_ed_edcat_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    df_ed_edcato_missing = df_ed[(df_ed["EDCAT"] == "是") & df_ed["EDCATO"].isna()]
    if not df_ed_edcato_missing.empty:
        df_query_list.append(create_query_ed(variable="EDCATO", label="其他分类", subjid=df_ed_edcato_missing["SUBJID"], query="【缺陷分类】选择“其他”，此字段必填"))

    df_ed_edacn_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDACN"].isna()]
    if not df_ed_edacn_missing.empty:
        df_query_list.append(create_query_ed(variable="EDACN", label="采取的措施", subjid=df_ed_edacn_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    df_ed_edstdat_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDSTDAT"].isna()]
    if not df_ed_edstdat_missing.empty:
        df_query_list.append(create_query_ed(variable="EDSTDAT", label="发生日期", subjid=df_ed_edstdat_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    df_ed_edaeyn_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDAEYN"].isna()]
    if not df_ed_edaeyn_missing.empty:
        df_query_list.append(create_query_ed(variable="EDAEYN", label="是否导致AE", subjid=df_ed_edaeyn_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    df_ed_edsaeyn_missing = df_ed[(df_ed["EDYN"] == "是") & df_ed["EDSAEYN"].isna()]
    if not df_ed_edsaeyn_missing.empty:
        df_query_list.append(create_query_ed(variable="EDSAEYN", label="是否导致SAE", subjid=df_ed_edsaeyn_missing["SUBJID"], query="【是否发生器械缺陷】选择“是”，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ed_logic(df_ed: pd.DataFrame, df_ct: pd.DataFrame, df_ds: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_ed = df_ed.loc[:, ["SUBJID", "EDSTDAT"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]
    df_ds = df_ds.loc[:, ["SUBJID", "DSYN", "DSCMPDAT", "DSENDAT"]]

    # 【发生日期】早于CT扫描【检查日期】，请核实
    df = df_ed.merge(df_ct, on="SUBJID", how="left")
    df_ed_logic = df[df["EDSTDAT"] < df["CTDAT"]]
    if not df_ed_logic.empty:
        df_query_list.append(create_query_ed(variable="EDSTDAT", label="发生日期", subjid=df_ed_logic["SUBJID"], query="【发生日期】早于CT扫描【检查日期】，请核实"))

    # 试验总结【是否完成整个临床试验】选择“是”，【发生日期】晚于试验总结【完成日期】，请核实
    df = df_ed.merge(df_ds, on="SUBJID", how="left")
    df_ed_logic = df[(df["DSYN"] == "是") & (df["EDSTDAT"] > df["DSCMPDAT"])]
    if not df_ed_logic.empty:
        df_query_list.append(create_query_ed(variable="EDSTDAT", label="发生日期", subjid=df_ed_logic["SUBJID"], query="试验总结【是否完成整个临床试验】选择“是”，【发生日期】晚于试验总结【完成日期】，请核实"))

    # 试验总结【是否完成整个临床试验】选择“否”，【发生日期】晚于试验总结【中止日期】，请核实
    df = df_ed.merge(df_ds, on="SUBJID", how="left")
    df_ed_logic = df[(df["DSYN"] == "否") & (df["EDSTDAT"] > df["DSENDAT"])]
    if not df_ed_logic.empty:
        df_query_list.append(create_query_ed(variable="EDSTDAT", label="发生日期", subjid=df_ed_logic["SUBJID"], query="试验总结【是否完成整个临床试验】选择“否”，【发生日期】晚于试验总结【中止日期】，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ed(df_ed: pd.DataFrame, df_ct: pd.DataFrame, df_ds: pd.DataFrame) -> pd.DataFrame:
    df_ed = df_ed[df_ed["EDYN"] == "是"]
    if df_ed.empty:
        return pd.DataFrame()

    df_query_list = []
    df_query_list.append(check_ed_missing(df_ed))
    df_query_list.append(check_ed_logic(df_ed, df_ct, df_ds))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
