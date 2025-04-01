import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V2"
formoid = "CT"
formnm = "CT扫描"

create_query_ctpar = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ctpar_missing(df_ctpar: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ctpar, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ctpar_seqspid_missing = df_ctpar[df_ctpar["SEQSPID"].isna()]
    if not df_ctpar_seqspid_missing.empty:
        df_query_list.append(create_query_ctpar(variable="SEQSPID", label="重建序列编号", subjid=df_ctpar_seqspid_missing["SUBJID"], query="此字段必填"))

    df_ctpar_slicdep_missing = df_ctpar[df_ctpar["SLICDEP"].isna()]
    if not df_ctpar_slicdep_missing.empty:
        df_query_list.append(create_query_ctpar(variable="SLICDEP", label="层厚(mm)", subjid=df_ctpar_slicdep_missing["SUBJID"], query="此字段必填"))

    df_ctpar_slicgap_missing = df_ctpar[df_ctpar["SLICGAP"].isna()]
    if not df_ctpar_slicgap_missing.empty:
        df_query_list.append(create_query_ctpar(variable="SLICGAP", label="层间距(mm)", subjid=df_ctpar_slicgap_missing["SUBJID"], query="此字段必填"))

    df_ctpar_kernal_missing = df_ctpar[df_ctpar["KERNAL"].isna()]
    if not df_ctpar_kernal_missing.empty:
        df_query_list.append(create_query_ctpar(variable="KERNAL", label="卷积核", subjid=df_ctpar_kernal_missing["SUBJID"], query="此字段必填"))

    df_ctpar_grmode_missing = df_ctpar[df_ctpar["GRMODE"].isna()]
    if not df_ctpar_grmode_missing.empty:
        df_query_list.append(create_query_ctpar(variable="GRMODE", label="降噪模式", subjid=df_ctpar_grmode_missing["SUBJID"], query="此字段必填"))

    df_ctpar_grlevl_missing = df_ctpar[df_ctpar["GRLEVL"].isna()]
    if not df_ctpar_grlevl_missing.empty:
        df_query_list.append(create_query_ctpar(variable="GRLEVL", label="降噪级别", subjid=df_ctpar_grlevl_missing["SUBJID"], query="此字段必填"))

    df_ctpar_fov_missing = df_ctpar[df_ctpar["FOV"].isna()]
    if not df_ctpar_fov_missing.empty:
        df_query_list.append(create_query_ctpar(variable="FOV", label="FOV(mm)", subjid=df_ctpar_fov_missing["SUBJID"], query="此字段必填"))

    df_ctpar_matrix_missing = df_ctpar[df_ctpar["MATRIX"].isna()]
    if not df_ctpar_matrix_missing.empty:
        df_query_list.append(create_query_ctpar(variable="MATRIX", label="矩阵", subjid=df_ctpar_matrix_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ctpar_logic(df_ctpar: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ctpar(df_ctpar: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_query_list.append(check_ctpar_missing(df_ctpar))
    df_query_list.append(check_ctpar_logic(df_ctpar))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
