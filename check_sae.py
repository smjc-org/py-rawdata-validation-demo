import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "COMPAGE"
formoid = "SAE"
formnm = "严重不良事件"

create_query_sae = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_sae_missing(df_sae: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_sae, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_sae_saeyn_missing = df_sae[df_sae["SAEYN"].isna()]
    if not df_sae_saeyn_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEYN", label="是否发生严重不良事件", subjid=df_sae_saeyn_missing["SUBJID"], query="此字段必填"))

    df_sae_saeno_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAENO"].isna()]
    if not df_sae_saeno_missing.empty:
        df_query_list.append(create_query_sae(variable="SAENO", label="编号", subjid=df_sae_saeno_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saeterm_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAETERM"].isna()]
    if not df_sae_saeterm_missing.empty:
        df_query_list.append(create_query_sae(variable="SAETERM", label="严重不良事件名称", subjid=df_sae_saeterm_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_reptyp_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["REPTYP"].isna()]
    if not df_sae_reptyp_missing.empty:
        df_query_list.append(create_query_sae(variable="REPTYP", label="报告类型", subjid=df_sae_reptyp_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_repdat_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["REPDAT"].isna()]
    if not df_sae_repdat_missing.empty:
        df_query_list.append(create_query_sae(variable="REPDAT", label="报告日期", subjid=df_sae_repdat_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saeudat_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEUDAT"].isna()]
    if not df_sae_saeudat_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEUDAT", label="使用日期", subjid=df_sae_saeudat_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saeodat_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEODAT"].isna()]
    if not df_sae_saeodat_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEODAT", label="发生日期", subjid=df_sae_saeodat_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saecat_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAECAT"].isna()]
    if not df_sae_saecat_missing.empty:
        df_query_list.append(create_query_sae(variable="SAECAT", label="严重不良事件分类", subjid=df_sae_saecat_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saecato_missing = df_sae[(df_sae["SAECAT"] == "其他") & df_sae["SAECATO"].isna()]
    if not df_sae_saecato_missing.empty:
        df_query_list.append(create_query_sae(variable="SAECATO", label="其他分类", subjid=df_sae_saecato_missing["SUBJID"], query="【严重不良事件分类】选择“其他”时，此字段必填"))

    df_sae_dthdat_missing = df_sae[((df_sae["SAEOUT"] == "死亡") | (df_sae["SAECAT"] == "导致死亡")) & df_sae["DTHDAT"].isna()]
    if not df_sae_dthdat_missing.empty:
        df_query_list.append(create_query_sae(variable="DTHDAT", label="死亡日期", subjid=df_sae_dthdat_missing["SUBJID"], query="【转归】选择“死亡”或【严重不良事件分类】选择“导致死亡”，此字段必填"))

    df_sae_saedacn_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEDACN"].isna()]
    if not df_sae_saedacn_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEDACN", label="对器械采取的措施", subjid=df_sae_saedacn_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saedacno_missing = df_sae[(df_sae["SAEDACN"] == "其他") & df_sae["SAEDACNO"].isna()]
    if not df_sae_saedacno_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEDACNO", label="其他措施", subjid=df_sae_saedacno_missing["SUBJID"], query="【对器械采取的措施】选择“其他”时，此字段必填"))

    df_sae_saeout_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEOUT"].isna()]
    if not df_sae_saeout_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEOUT", label="转归", subjid=df_sae_saeout_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saeouto_missing = df_sae[(df_sae["SAEOUT"] == "其他") & df_sae["SAEOUTO"].isna()]
    if not df_sae_saeouto_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEOUTO", label="其他转归", subjid=df_sae_saeouto_missing["SUBJID"], query="【转归】选择“其他”时，此字段必填"))

    df_sae_saedrel_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEDREL"].isna()]
    if not df_sae_saedrel_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEDREL", label="与试验医疗器械的关系", subjid=df_sae_saedrel_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saeedyn_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEEDYN"].isna()]
    if not df_sae_saeedyn_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEEDYN", label="是否器械缺陷", subjid=df_sae_saeedyn_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_expyn_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["EXPYN"].isna()]
    if not df_sae_expyn_missing.empty:
        df_query_list.append(create_query_sae(variable="EXPYN", label="是否预期", subjid=df_sae_expyn_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_rinfoyn_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["RINFOYN"].isna()]
    if not df_sae_rinfoyn_missing.empty:
        df_query_list.append(create_query_sae(variable="RINFOYN", label="是否其他严重安全性风险信息", subjid=df_sae_rinfoyn_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saewyn_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEWYN"].isna()]
    if not df_sae_saewyn_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEWYN", label="是否大范围严重不良事件或其他重大安全性问题", subjid=df_sae_saewyn_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saeacdes_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAEACDES"].isna()]
    if not df_sae_saeacdes_missing.empty:
        df_query_list.append(create_query_sae(variable="SAEACDES", label="发生以及处理详细情况", subjid=df_sae_saeacdes_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saercacn_missing = df_sae[(df_sae["SAEYN"] == "是") & df_sae["SAERCACN"].isna()]
    if not df_sae_saercacn_missing.empty:
        df_query_list.append(create_query_sae(variable="SAERCACN", label="采取何种风险控制措施", subjid=df_sae_saercacn_missing["SUBJID"], query="【是否发生严重不良事件】选择“是”时，此字段必填"))

    df_sae_saercoth_missing = df_sae[(df_sae["SAERCACN"] == "其他") & df_sae["SAERCOTH"].isna()]
    if not df_sae_saercoth_missing.empty:
        df_query_list.append(create_query_sae(variable="SAERCOTH", label="其他风险控制措施", subjid=df_sae_saercoth_missing["SUBJID"], query="【采取何种风险控制措施】选择“其他”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_sae_logic(df_sae: pd.DataFrame, df_info: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_sae = df_sae.loc[:, ["SUBJID", "SAEODAT", "DTHDAT"]]
    df_info = df_info.loc[:, ["SUBJID", "ICFDAT"]]

    # 【发生日期】早于【知情同意签署日期】，请核实
    df = df_sae.merge(df_info, on="SUBJID", how="left")
    df_sae_logic = df[df["SAEODAT"] < df["ICFDAT"]]
    if not df_sae_logic.empty:
        df_query_list.append(create_query_sae(variable="SAEODAT", label="发生日期", subjid=df_sae_logic["SUBJID"], query="【发生日期】早于【知情同意签署日期】，请核实"))

    # 【死亡日期】早于【发生日期】，请核实
    df = df_sae
    df_sae_logic = df[df["DTHDAT"] < df["SAEODAT"]]
    if not df_sae_logic.empty:
        df_query_list.append(create_query_sae(variable="DTHDAT", label="死亡日期", subjid=df_sae_logic["SUBJID"], query="【死亡日期】早于【发生日期】，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_sae(df_sae: pd.DataFrame, df_info: pd.DataFrame) -> pd.DataFrame:
    df_sae = df_sae[df_sae["SAEYN"] == "是"]
    if df_sae.empty:
        return pd.DataFrame()

    df_query_list = []
    df_query_list.append(check_sae_missing(df_sae))
    df_query_list.append(check_sae_logic(df_sae, df_info))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
