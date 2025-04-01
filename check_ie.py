import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V1"
formoid = "IE"
formnm = "入选排除筛选"

create_query_ie = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ie_missing(df_ie: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ie, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ie_ectcyn_missing = df_ie[df_ie["ECTCYN"].isna()]
    if not df_ie_ectcyn_missing.empty:
        df_query_list.append(create_query_ie(variable="ECTCYN", label="初拟增强检查经筛选后是否有变更", subjid=df_ie_ectcyn_missing["SUBJID"], query="此字段必填"))

    df_ie_grpyn_missing = df_ie[df_ie["GRPYN"].isna()]
    if not df_ie_grpyn_missing.empty:
        df_query_list.append(create_query_ie(variable="GRPYN", label="是否入组", subjid=df_ie_grpyn_missing["SUBJID"], query="此字段必填"))

    df_ie_ugpreas_missing = df_ie[(df_ie["GRPYN"] == "否") & df_ie["UGPREAS"].isna()]
    if not df_ie_ugpreas_missing.empty:
        df_query_list.append(create_query_ie(variable="UGPREAS", label="未入组原因", subjid=df_ie_ugpreas_missing["SUBJID"], query="【是否入组】选择“否”时，此字段必填"))

    df_ie_grpdat_missing = df_ie[(df_ie["GRPYN"] == "是") & df_ie["GRPDAT"].isna()]
    if not df_ie_grpdat_missing.empty:
        df_query_list.append(create_query_ie(variable="GRPDAT", label="入组日期", subjid=df_ie_grpdat_missing["SUBJID"], query="【是否入组】选择“是”时，此字段必填"))

    df_ie_grpid_missing = df_ie[(df_ie["GRPYN"] == "是") & df_ie["GRPID"].isna()]
    if not df_ie_grpid_missing.empty:
        df_query_list.append(create_query_ie(variable="GRPID", label="入组号", subjid=df_ie_grpid_missing["SUBJID"], query="【是否入组】选择“是”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ie_logic(df_ie: pd.DataFrame, df_in: pd.DataFrame, df_ex: pd.DataFrame, df_info: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_ie = df_ie.loc[:, ["SUBJID", "GRPYN", "GRPDAT"]]
    df_info = df_info.loc[:, ["SUBJID", "ICFDAT"]]
    df_in = df_in.loc[:, ["SUBJID", "INSPID", "INYN"]]
    df_ex = df_ex.loc[:, ["SUBJID", "EXSPID", "EXYN"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]

    # 入选标准有选“否”或排除标准有选“是”，【是否入组】选择“是”，请核实
    df_in_gb = df_in.groupby("SUBJID")["INYN"].apply(lambda x: any(x == "否"))
    df_ex_gb = df_ex.groupby("SUBJID")["EXYN"].apply(lambda x: any(x == "是"))
    df = df_ie.merge(df_in_gb, on="SUBJID", how="left").merge(df_ex_gb, on="SUBJID", how="left")
    df_ie_logic = df[(df["INYN"] | df["EXYN"]) & (df["GRPYN"] == "是")]
    if not df_ie_logic.empty:
        df_query_list.append(create_query_ie(variable="GRPYN", label="是否入组", subjid=df_ie_logic["SUBJID"], query="入选标准有选“否”或排除标准有选“是”，【是否入组】选择“是”，请核实"))

    # 入选标准均选“是”且排除标准不存在选“是”，【是否入组】选择“否”，请核实
    df_in_gb = df_in.groupby("SUBJID")["INYN"].apply(lambda x: all(x == "是"))
    df_ex_gb = df_ex.groupby("SUBJID")["EXYN"].apply(lambda x: not any(x == "是"))
    df = df_ie.merge(df_in_gb, on="SUBJID", how="left").merge(df_ex_gb, on="SUBJID", how="left")
    df_ie_logic = df[(df["INYN"] & df["EXYN"]) & (df["GRPYN"] == "否")]
    if not df_ie_logic.empty:
        df_query_list.append(create_query_ie(variable="GRPYN", label="是否入组", subjid=df_ie_logic["SUBJID"], query="入选标准均选“是”且排除标准不存在选“是”，【是否入组】选择“否”，请核实"))

    # 【入组日期】早于【知情同意签署日期】或晚于CT【检查日期】，请核实
    df = df_ie.merge(df_info, on="SUBJID", how="left").merge(df_ct, on="SUBJID", how="left")
    df_ie_logic = df[(df["GRPDAT"] < df["ICFDAT"]) | (df["GRPDAT"] > df["CTDAT"])]
    if not df_ie_logic.empty:
        df_query_list.append(create_query_ie(variable="GRPDAT", label="入组日期", subjid=df_ie_logic["SUBJID"], query="【入组日期】早于【知情同意签署日期】或晚于CT【检查日期】，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ie(df_ie: pd.DataFrame, df_in: pd.DataFrame, df_ex: pd.DataFrame, df_info: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_query_list.append(check_ie_missing(df_ie))
    df_query_list.append(check_ie_logic(df_ie, df_in, df_ex, df_info, df_ct))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
