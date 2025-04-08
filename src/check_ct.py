import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V2"
formoid = "CT"
formnm = "CT扫描"

create_query_ct = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ct_missing(df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ct, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ct_ctdat_missing = df_ct[df_ct["CTDAT"].isna()]
    if not df_ct_ctdat_missing.empty:
        df_query_list.append(create_query_ct(variable="CTDAT", label="检查日期", subjid=df_ct_ctdat_missing["SUBJID"], query="此字段必填"))

    df_ct_ctloc_missing = df_ct[df_ct["CTLOC"].isna()]
    if not df_ct_ctloc_missing.empty:
        df_query_list.append(create_query_ct(variable="CTLOC", label="扫描部位", subjid=df_ct_ctloc_missing["SUBJID"], query="此字段必填"))

    df_ct_ctsite_missing = df_ct[df_ct["CTSITE"].isna()]
    if not df_ct_ctsite_missing.empty:
        df_query_list.append(create_query_ct(variable="CTSITE", label="具体部位", subjid=df_ct_ctsite_missing["SUBJID"], query="此字段必填"))

    df_ct_cttyp_missing = df_ct[df_ct["CTTYP"].isna()]
    if not df_ct_cttyp_missing.empty:
        df_query_list.append(create_query_ct(variable="CTTYP", label="扫描方式", subjid=df_ct_cttyp_missing["SUBJID"], query="此字段必填"))

    df_ct_cgterm_missing = df_ct[(df_ct["CTTYP"] == "增强扫描") & df_ct["CGTERM"].isna()]
    if not df_ct_cgterm_missing.empty:
        df_query_list.append(create_query_ct(variable="CGTERM", label="造影剂名称", subjid=df_ct_cgterm_missing["SUBJID"], query="【扫描方式】选择“增强扫描”时，此字段必填"))

    df_ct_cgcon_missing = df_ct[(df_ct["CTTYP"] == "增强扫描") & df_ct["CGCON"].isna()]
    if not df_ct_cgcon_missing.empty:
        df_query_list.append(create_query_ct(variable="CGCON", label="造影剂浓度(mg I/ml)", subjid=df_ct_cgcon_missing["SUBJID"], query="【扫描方式】选择“增强扫描”时，此字段必填"))

    df_ct_cgdose_missing = df_ct[(df_ct["CTTYP"] == "增强扫描") & df_ct["CGDOSE"].isna()]
    if not df_ct_cgdose_missing.empty:
        df_query_list.append(create_query_ct(variable="CGDOSE", label="造影剂剂量(ml)", subjid=df_ct_cgdose_missing["SUBJID"], query="【扫描方式】选择“增强扫描”时，此字段必填"))

    df_ct_cgfrat_missing = df_ct[(df_ct["CTTYP"] == "增强扫描") & df_ct["CGFRAT"].isna()]
    if not df_ct_cgfrat_missing.empty:
        df_query_list.append(create_query_ct(variable="CGFRAT", label="造影剂流速(ml/s)", subjid=df_ct_cgfrat_missing["SUBJID"], query="【扫描方式】选择“增强扫描”时，此字段必填"))

    df_ct_sccat_missing = df_ct[df_ct["SCCAT"].isna()]
    if not df_ct_sccat_missing.empty:
        df_query_list.append(create_query_ct(variable="SCCAT", label="扫描类型", subjid=df_ct_sccat_missing["SUBJID"], query="此字段必填"))

    df_ct_colmwid_missing = df_ct[df_ct["COLMWID"].isna()]
    if not df_ct_colmwid_missing.empty:
        df_query_list.append(create_query_ct(variable="COLMWID", label="准直宽度(mm)", subjid=df_ct_colmwid_missing["SUBJID"], query="此字段必填"))

    df_ct_tubvolt_missing = df_ct[df_ct["TUBVOLT"].isna()]
    if not df_ct_tubvolt_missing.empty:
        df_query_list.append(create_query_ct(variable="TUBVOLT", label="管电压", subjid=df_ct_tubvolt_missing["SUBJID"], query="此字段必填"))

    df_ct_tubcurr_missing = df_ct[df_ct["TUBCURR"].isna()]
    if not df_ct_tubcurr_missing.empty:
        df_query_list.append(create_query_ct(variable="TUBCURR", label="管电流", subjid=df_ct_tubcurr_missing["SUBJID"], query="此字段必填"))

    df_ct_rps_missing = df_ct[df_ct["RPS"].isna()]
    if not df_ct_rps_missing.empty:
        df_query_list.append(create_query_ct(variable="RPS", label="转速(秒)", subjid=df_ct_rps_missing["SUBJID"], query="此字段必填"))

    df_ct_screpit_missing = df_ct[(df_ct["SCCAT"] == "螺旋") & df_ct["SCREPIT"].isna()]
    if not df_ct_screpit_missing.empty:
        df_query_list.append(create_query_ct(variable="SCREPIT", label="螺距", subjid=df_ct_screpit_missing["SUBJID"], query="【扫描类型】选择“螺旋”时，此字段必填"))

    df_ct_mas_missing = df_ct[df_ct["MAS"].isna()]
    if not df_ct_mas_missing.empty:
        df_query_list.append(create_query_ct(variable="MAS", label="mAs", subjid=df_ct_mas_missing["SUBJID"], query="此字段必填"))

    df_ct_extim_missing = df_ct[df_ct["EXTIM"].isna()]
    if not df_ct_extim_missing.empty:
        df_query_list.append(create_query_ct(variable="EXTIM", label="曝光时间", subjid=df_ct_extim_missing["SUBJID"], query="此字段必填"))

    df_ct_ctdi_missing = df_ct[df_ct["CTDI"].isna()]
    if not df_ct_ctdi_missing.empty:
        df_query_list.append(create_query_ct(variable="CTDI", label="CTDIw/CTDIvol（mGy)", subjid=df_ct_ctdi_missing["SUBJID"], query="此字段必填"))

    df_ct_dlp_missing = df_ct[df_ct["DLP"].isna()]
    if not df_ct_dlp_missing.empty:
        df_query_list.append(create_query_ct(variable="DLP", label="DLP(mGy.cm)", subjid=df_ct_dlp_missing["SUBJID"], query="此字段必填"))

    df_ct_ctodyn_missing = df_ct[df_ct["CTODYN"].isna()]
    if not df_ct_ctodyn_missing.empty:
        df_query_list.append(create_query_ct(variable="CTODYN", label="扫描剂量是否超出参考水平", subjid=df_ct_ctodyn_missing["SUBJID"], query="此字段必填"))

    df_ct_odreas_missing = df_ct[(df_ct["CTODYN"] == "是") & df_ct["ODREAS"].isna()]
    if not df_ct_odreas_missing.empty:
        df_query_list.append(create_query_ct(variable="ODREAS", label="超出原因", subjid=df_ct_odreas_missing["SUBJID"], query="【扫描剂量是否超出参考水平】选择“是”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ct_logic(df_ct: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_ct = df_ct.loc[:, ["SUBJID", "CTTYP"]]
    df_ie = df_ie.loc[:, ["SUBJID", "ECTCYN"]]

    # 【初拟增强检查经筛选后是否有变更】选择“无”，【扫描方式】选择“平扫”，请核实
    df = df_ct.merge(df_ie, on="SUBJID", how="left")
    df_ct_logic = df[(df["ECTCYN"] == "无") & (df["CTTYP"] == "平扫")]
    if not df_ct_logic.empty:
        df_query_list.append(create_query_ct(variable="CTTYP", label="扫描方式", subjid=df_ct_logic["SUBJID"], query="【初拟增强检查经筛选后是否有变更】选择“无”，【扫描方式】选择“平扫”，请核实"))

    # 【初拟增强检查经筛选后是否有变更】选择“增强改平扫”，【扫描方式】选择“增强扫描”，请核实
    df = df_ct.merge(df_ie, on="SUBJID", how="left")
    df_ct_logic = df[(df["ECTCYN"] == "增强改平扫") & (df["CTTYP"] == "增强扫描")]
    if not df_ct_logic.empty:
        df_query_list.append(create_query_ct(variable="CTTYP", label="扫描方式", subjid=df_ct_logic["SUBJID"], query="【初拟增强检查经筛选后是否有变更】选择“增强改平扫”，【扫描方式】选择“增强扫描”，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ct(df_ct: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_ct = df_ct.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_ct_missing(df_ct))
    df_query_list.append(check_ct_logic(df_ct, df_ie))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
