import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V1"
formoid = "PH"
formnm = "既往史及个人史"

create_query_ph = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ph_missing(df_ph: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ph, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ph_phspid_missing = df_ph[df_ph["PHSPID"].isna()]
    if not df_ph_phspid_missing.empty:
        df_query_list.append(create_query_ph(variable="PHSPID", label="记录号", subjid=df_ph_phspid_missing["SUBJID"], query="此字段必填"))

    df_ph_phterm_missing = df_ph[df_ph["PHTERM"].isna()]
    if not df_ph_phterm_missing.empty:
        df_query_list.append(create_query_ph(variable="PHTERM", label="既往史描述", subjid=df_ph_phterm_missing["SUBJID"], query="此字段必填"))

    df_ph_phyn_missing = df_ph[df_ph["PHTERM"].notna() & (df_ph["PHTERM"] != "其他") & df_ph["PHYN"].isna()]
    if not df_ph_phyn_missing.empty:
        df_query_list.append(create_query_ph(variable="PHYN", label="结果", subjid=df_ph_phyn_missing["SUBJID"], query="【既往史描述】不为空，且未选择“其他”时，此字段必填"))

    df_ph_photh_missing = df_ph[(df_ph["PHTERM"] == "其他") & df_ph["PHOTH"].isna()]
    if not df_ph_photh_missing.empty:
        df_query_list.append(create_query_ph(variable="PHOTH", label="其他既往史", subjid=df_ph_photh_missing["SUBJID"], query="【既往史描述】选择“其他”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ph_logic(df_ph: pd.DataFrame, df_info: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_ph = df_ph.loc[:, ["SUBJID", "PHSPID", "PHYN"]]
    df_info = df_info.loc[:, ["SUBJID", "SEX"]]
    df_ie = df_ie.loc[:, ["SUBJID", "ECTCYN"]]

    # 【性别】选择“男”，既往史描述3【女性是否在哺乳期】未选择“不适用”，请核实
    df = df_ph.merge(df_info, on="SUBJID", how="left")
    df_ph_logic = df[(df["SEX"] == "男") & (df["PHSPID"] == 3) & (df["PHYN"] != "不适用")]
    if not df_ph_logic.empty:
        df_query_list.append(create_query_ph(variable="PHTERM", label="结果", subjid=df_ph_logic["SUBJID"], query="【性别】选择“男”，既往史描述3【女性是否在哺乳期】未选择“不适用”，请核实"))

    # 【性别】选择“女”，既往史描述3【女性是否在哺乳期】选择“不适用”，请核实
    df = df_ph.merge(df_info, on="SUBJID", how="left")
    df_ph_logic = df[(df["SEX"] == "女") & (df["PHSPID"] == 3) & (df["PHYN"] == "不适用")]
    if not df_ph_logic.empty:
        df_query_list.append(create_query_ph(variable="PHTERM", label="结果", subjid=df_ph_logic["SUBJID"], query="【性别】选择“女”，既往史描述3【女性是否在哺乳期】选择“不适用”，请核实"))

    # 【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，既往史6【结果】选择“不适用”，请核实
    df = df_ph.merge(df_ie, on="SUBJID", how="left")
    df_ph_logic = df[df["ECTCYN"].notna() & (df["ECTCYN"] != "不适用") & (df["PHSPID"] == 6) & (df["PHYN"] == "不适用")]
    if not df_ph_logic.empty:
        df_query_list.append(create_query_ph(variable="PHTERM", label="结果", subjid=df_ph_logic["SUBJID"], query="【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，既往史6【结果】选择“不适用”，请核实"))

    # 【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，既往史7【结果】选择“不适用”，请核实
    df = df_ph.merge(df_ie, on="SUBJID", how="left")
    df_ph_logic = df[df["ECTCYN"].notna() & (df["ECTCYN"] != "不适用") & (df["PHSPID"] == 7) & (df["PHYN"] == "不适用")]
    if not df_ph_logic.empty:
        df_query_list.append(create_query_ph(variable="PHTERM", label="结果", subjid=df_ph_logic["SUBJID"], query="【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，既往史7【结果】选择“不适用”，请核实"))

    # 【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，既往史8【结果】选择“不适用”，请核实
    df = df_ph.merge(df_ie, on="SUBJID", how="left")
    df_ph_logic = df[df["ECTCYN"].notna() & (df["ECTCYN"] != "不适用") & (df["PHSPID"] == 8) & (df["PHYN"] == "不适用")]
    if not df_ph_logic.empty:
        df_query_list.append(create_query_ph(variable="PHTERM", label="结果", subjid=df_ph_logic["SUBJID"], query="【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，既往史8【结果】选择“不适用”，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ph(df_ph: pd.DataFrame, df_info: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_ph = df_ph.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_ph_missing(df_ph))
    df_query_list.append(check_ph_logic(df_ph, df_info, df_ie))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
