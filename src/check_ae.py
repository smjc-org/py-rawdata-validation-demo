import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "COMPAGE"
formoid = "AE"
formnm = "不良事件"

create_query_ae = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ae_missing(df_ae: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ae, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ae_aeyn_missing = df_ae[df_ae["AEYN"].isna()]
    if not df_ae_aeyn_missing.empty:
        df_query_list.append(create_query_ae(variable="AEYN", label="是否发生不良事件", subjid=df_ae_aeyn_missing["SUBJID"], query="此字段必填"))

    df_ae_aeno_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AENO"].isna()]
    if not df_ae_aeno_missing.empty:
        df_query_list.append(create_query_ae(variable="AENO", label="编号", subjid=df_ae_aeno_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aeterm_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AETERM"].isna()]
    if not df_ae_aeterm_missing.empty:
        df_query_list.append(create_query_ae(variable="AETERM", label="不良事件名称", subjid=df_ae_aeterm_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aestdat_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AESTDAT"].isna()]
    if not df_ae_aestdat_missing.empty:
        df_query_list.append(create_query_ae(variable="AESTDAT", label="开始日期", subjid=df_ae_aestdat_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aeongo_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEONGO"].isna()]
    if not df_ae_aeongo_missing.empty:
        df_query_list.append(create_query_ae(variable="AEONGO", label="是否持续", subjid=df_ae_aeongo_missing["SUBJID"], query="【是否持续】选择“否”时，此字段必填"))

    df_ae_aeendat_missing = df_ae[(df_ae["AEONGO"] == "否") & df_ae["AEENDAT"].isna()]
    if not df_ae_aeendat_missing.empty:
        df_query_list.append(create_query_ae(variable="AEENDAT", label="结束日期", subjid=df_ae_aeendat_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aesev_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AESEV"].isna()]
    if not df_ae_aesev_missing.empty:
        df_query_list.append(create_query_ae(variable="AESEV", label="严重程度", subjid=df_ae_aesev_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aeacn_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEACN"].isna()]
    if not df_ae_aeacn_missing.empty:
        df_query_list.append(create_query_ae(variable="AEACN", label="对不良事件采取的措施", subjid=df_ae_aeacn_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aeacno_missing = df_ae[(df_ae["AEACN"] == "其他") & df_ae["AEACNO"].isna()]
    if not df_ae_aeacno_missing.empty:
        df_query_list.append(create_query_ae(variable="AEACNO", label="其他治疗", subjid=df_ae_aeacno_missing["SUBJID"], query="【对不良事件采取的措施】选择“其他治疗”时，此字段必填"))

    df_ae_aedrel_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEDREL"].isna()]
    if not df_ae_aedrel_missing.empty:
        df_query_list.append(create_query_ae(variable="AEDREL", label="与试验医疗器械的关系", subjid=df_ae_aedrel_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aedacn_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEDACN"].isna()]
    if not df_ae_aedacn_missing.empty:
        df_query_list.append(create_query_ae(variable="AEDACN", label="对试验医疗器械采取的措施", subjid=df_ae_aedacn_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aedacno_missing = df_ae[(df_ae["AEDACN"] == "其他") & df_ae["AEDACNO"].isna()]
    if not df_ae_aedacno_missing.empty:
        df_query_list.append(create_query_ae(variable="AEDACNO", label="其他措施", subjid=df_ae_aedacno_missing["SUBJID"], query="【对试验医疗器械采取的措施】选择“其他”时，此字段必填"))

    df_ae_aeout_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEOUT"].isna()]
    if not df_ae_aeout_missing.empty:
        df_query_list.append(create_query_ae(variable="AEOUT", label="转归", subjid=df_ae_aeout_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aeouto_missing = df_ae[(df_ae["AEOUT"] == "其他") & df_ae["AEOUTO"].isna()]
    if not df_ae_aeouto_missing.empty:
        df_query_list.append(create_query_ae(variable="AEOUTO", label="其他转归", subjid=df_ae_aeouto_missing["SUBJID"], query="【转归】选择“其他”时，此字段必填"))

    df_ae_saeyn_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AESER"].isna()]
    if not df_ae_saeyn_missing.empty:
        df_query_list.append(create_query_ae(variable="SAEYN", label="是否为严重不良事件", subjid=df_ae_saeyn_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_saesutn_missing = df_ae[(df_ae["AESER"] == "是") & df_ae["SAESUTN"].isna()]
    if not df_ae_saesutn_missing.empty:
        df_query_list.append(create_query_ae(variable="SAESUTN", label="严重不良事件具体类型", subjid=df_ae_saesutn_missing["SUBJID"], query="【是否为严重不良事件】选择“是”时，此字段必填"))

    df_ae_saesutno_missing = df_ae[(df_ae["SAESUTN"] == "其他") & df_ae["SAESUTNO"].isna()]
    if not df_ae_saesutno_missing.empty:
        df_query_list.append(create_query_ae(variable="SAESUTNO", label="其他类型", subjid=df_ae_saesutno_missing["SUBJID"], query="【严重不良事件具体类型】选择“其他”时，此字段必填"))

    df_ae_deathdat_missing = df_ae[((df_ae["AEOUT"] == "死亡") | (df_ae["SAESUTN"] == "导致死亡")) & df_ae["DEATHDAT"].isna()]
    if not df_ae_deathdat_missing.empty:
        df_query_list.append(create_query_ae(variable="DEATHDAT", label="死亡日期", subjid=df_ae_deathdat_missing["SUBJID"], query="【转归】选择“死亡”或【严重不良事件具体类型】选择“导致死亡”时，此字段必填"))

    df_ae_aedis_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEDIS"].isna()]
    if not df_ae_aedis_missing.empty:
        df_query_list.append(create_query_ae(variable="AEDIS", label="是否因此AE导致试验中止", subjid=df_ae_aedis_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    df_ae_aeacdes_missing = df_ae[(df_ae["AEYN"] == "是") & df_ae["AEACDES"].isna()]
    if not df_ae_aeacdes_missing.empty:
        df_query_list.append(create_query_ae(variable="AEACDES", label="发生以及处理详细情况", subjid=df_ae_aeacdes_missing["SUBJID"], query="【是否发生不良事件】选择“是”时，此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ae_logic(df_ae: pd.DataFrame, df_ds: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_ae = df_ae.loc[:, ["SUBJID", "AESTDAT", "AEDREL", "AEENDAT", "AEOUT", "AESER", "AEDIS"]]
    df_ds = df_ds.loc[:, ["SUBJID", "DSYN", "DSCMPDAT", "DSENDAT"]]
    df_ct = df_ct.loc[:, ["SUBJID", "CTDAT"]]

    # 试验总结【是否完成整个临床试验】选择“是”，不良事件【开始日期】晚于试验总结【完成日期】，请核实
    df = df_ae.merge(df_ds, on="SUBJID", how="left")
    df_ae_logic = df[(df["DSYN"] == "是") & (df["AESTDAT"] > df["DSCMPDAT"])]
    if not df_ae_logic.empty:
        df_query_list.append(create_query_ae(variable="AESTDAT", label="开始日期", subjid=df_ae_logic["SUBJID"], query="【是否完成整个临床试验】选择“是”，不良事件【开始日期】晚于试验总结【完成日期】，请核实"))

    # 试验总结【是否完成整个临床试验】选择“否”，不良事件【开始日期】晚于试验总结【中止日期】，请核实
    df = df_ae.merge(df_ds, on="SUBJID", how="left")
    df_ae_logic = df[(df["DSYN"] == "否") & (df["AESTDAT"] > df["DSENDAT"])]
    if not df_ae_logic.empty:
        df_query_list.append(create_query_ae(variable="AESTDAT", label="开始日期", subjid=df_ae_logic["SUBJID"], query="【是否完成整个临床试验】选择“否”，不良事件【开始日期】晚于试验总结【中止日期】，请核实"))

    # 不良事件【结束日期】早于CT扫描【检查日期】，【与试验医疗器械的关系】未选择【肯定无关】，请核实
    df = df_ae.merge(df_ct, on="SUBJID", how="left")
    df_ae_logic = df[(df["AEENDAT"] < df["CTDAT"]) & (df["AEDREL"] != "肯定无关")]
    if not df_ae_logic.empty:
        df_query_list.append(create_query_ae(variable="AEDREL", label="与试验医疗器械的关系", subjid=df_ae_logic["SUBJID"], query="【结束日期】早于CT扫描【检查日期】，【与试验医疗器械的关系】未选择【肯定无关】，请核实"))

    # 不良事件【结束日期】早于【开始日期】，请核实
    df = df_ae
    df_ae_logic = df[df["AEENDAT"] < df["AESTDAT"]]
    if not df_ae_logic.empty:
        df_query_list.append(create_query_ae(variable="AEENDAT", label="结束日期", subjid=df_ae_logic["SUBJID"], query="【结束日期】早于【开始日期】，请核实"))

    # 不良事件【转归】选择“死亡”，【是否为严重不良事件】选择“否”，请核实
    df = df_ae
    df_ae_logic = df[(df["AEOUT"] == "死亡") & (df["AESER"] == "否")]
    if not df_ae_logic.empty:
        df_query_list.append(create_query_ae(variable="AESER", label="是否为严重不良事件", subjid=df_ae_logic["SUBJID"], query="【转归】选择“死亡”，【是否为严重不良事件】选择“否”，请核实"))

    # 【是否因此AE导致试验中止】选择“是”，试验总结【是否完成整个临床试验】选择“是”，请核实
    df = df_ae.merge(df_ds, on="SUBJID", how="left")
    df_ae_logic = df[(df["AEDIS"] == "是") & (df["DSYN"] == "是")]
    if not df_ae_logic.empty:
        df_query_list.append(create_query_ae(variable="AEDIS", label="是否因此AE导致试验中止", subjid=df_ae_logic["SUBJID"], query="【是否因此AE导致试验中止】选择“是”，试验总结【是否完成整个临床试验】选择“是”，请核实"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ae(df_ae: pd.DataFrame, df_ds: pd.DataFrame, df_ct: pd.DataFrame) -> pd.DataFrame:
    df_ae = df_ae[df_ae["AEYN"] == "是"]
    if df_ae.empty:
        return pd.DataFrame()

    df_query_list = []
    df_query_list.append(check_ae_missing(df_ae))
    df_query_list.append(check_ae_logic(df_ae, df_ds, df_ct))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
