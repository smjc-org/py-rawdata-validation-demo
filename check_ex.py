import pandas as pd

from functools import partial

from query import create_query
from check_common import check_common

vistoid = "V1"
formoid = "EX"
formnm = "排除标准"

create_query_ex = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)


# 空值核查
def check_ex_missing(df_ex: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []

    df_query_list.append(check_common(df=df_ex, vistoid=vistoid, formoid=formoid, formnm=formnm))

    df_ex_exspid_missing = df_ex[df_ex["EXSPID"].isna()]
    if not df_ex_exspid_missing.empty:
        df_query_list.append(create_query_ex(variable="EXSPID", label="记录号", subjid=df_ex_exspid_missing["SUBJID"], query="此字段必填"))

    df_ex_exterm_missing = df_ex[df_ex["EXTERM"].isna()]
    if not df_ex_exterm_missing.empty:
        df_query_list.append(create_query_ex(variable="EXTERM", label="排除标准描述", subjid=df_ex_exterm_missing["SUBJID"], query="此字段必填"))

    df_ex_exyn_missing = df_ex[df_ex["EXYN"].isna()]
    if not df_ex_exyn_missing.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_exyn_missing["SUBJID"], query="此字段必填"))

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


# 逻辑核查
def check_ex_logic(df_ex: pd.DataFrame, df_info: pd.DataFrame, df_lb: pd.DataFrame, df_ph: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_query_list = []
    df_ex = df_ex.loc[:, ["SUBJID", "EXSPID", "EXYN"]]
    df_info = df_info.loc[:, ["SUBJID", "HEIGHT", "WEIGHT"]]
    df_lb = df_lb.loc[:, ["SUBJID", "LBTERM", "LBORRES"]]
    df_ph = df_ph.loc[:, ["SUBJID", "PHSPID", "PHYN"]]
    df_ie = df_ie.loc[:, ["SUBJID", "ECTCYN"]]

    # 【体重】≥150Kg，排除标准1【体重≥150Kg】选择“否”，请核实
    df = df_ex.merge(df_info, on="SUBJID", how="left")
    df_ex_logic = df[(df["WEIGHT"] >= 150) & (df["EXSPID"] == 1) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_logic["SUBJID"], query="【体重】≥150Kg，排除标准1【体重≥150Kg】选择“否”，请核实"))

    # 【体重】＜150Kg，排除标准1【体重≥150Kg】选择“是”，请核实
    df = df_ex.merge(df_info, on="SUBJID", how="left")
    df_ex_logic = df[(df["WEIGHT"] < 150) & (df["EXSPID"] == 1) & (df["EXYN"] == "是")]
    if not df_ex_logic.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_logic["SUBJID"], query="【体重】＜150Kg，排除标准1【体重≥150Kg】选择“是”，请核实"))

    # 【身高】＞190cm，排除标准2【身高高于1.9米】选择“否”，请核实
    df = df_ex.merge(df_info, on="SUBJID", how="left")
    df_ex_logic = df[(df["HEIGHT"] > 190) & (df["EXSPID"] == 2) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_logic["SUBJID"], query="【身高】＞190cm，排除标准2【身高高于1.9米】选择“否”，请核实"))

    # 【身高】≤190cm，排除标准2【身高高于1.9米】选择“是”，请核实
    df = df_ex.merge(df_info, on="SUBJID", how="left")
    df_ex_logic = df[(df["HEIGHT"] <= 190) & (df["EXSPID"] == 2) & (df["EXYN"] == "是")]
    if not df_ex_logic.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_logic["SUBJID"], query="【身高】≤190cm，排除标准2【身高高于1.9米】选择“是”，请核实"))

    # 血妊娠【检查结果】为“阳性”，排除标准4【妊娠或哺乳期女性以及未来6个月计划怀孕的受试者（男性患者及育龄期女性患者在签署研究知情同意书起至检查结束后6个月内，必顼进行充分的避孕措施）】选择“否”，请核实
    df = df_ex.merge(df_lb, on="SUBJID", how="left")
    df_ex_logic = df[(df["LBTERM"] == "血妊娠") & (df["LBORRES"] == "阳性") & (df["EXSPID"] == 4) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="血妊娠【检查结果】为“阳性”，排除标准4【妊娠或哺乳期女性以及未来6个月计划怀孕的受试者（男性患者及育龄期女性患者在签署研究知情同意书起至检查结束后6个月内，必顼进行充分的避孕措施）】选择“否”，请核实",
            )
        )

    # 既往史描述1【是否未来6个月内计划怀孕】选择“是”，排除标准4【妊娠或哺乳期女性以及未来6个月计划怀孕的受试者（男性患者及育龄期女性患者在签署研究知情同意书起至检查结束后6个月内，必顼进行充分的避孕措施）】选择“否”，请核实
    df = df_ex.merge(df_ph, on="SUBJID", how="left")
    df_ex_logic = df[(df["PHSPID"] == 1) & (df["PHYN"] == "是") & (df["EXSPID"] == 4) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="既往史描述1【是否未来6个月内计划怀孕】选择“是”，排除标准4【妊娠或哺乳期女性以及未来6个月计划怀孕的受试者（男性患者及育龄期女性患者在签署研究知情同意书起至检查结束后6个月内，必顼进行充分的避孕措施）】选择“否”，请核实",
            )
        )

    # 既往史描述3【女性是否在哺乳期】选择“是”，排除标准4【妊娠或哺乳期女性以及未来6个月计划怀孕的受试者（男性患者及育龄期女性患者在签署研究知情同意书起至检查结束后6个月内，必顼进行充分的避孕措施）】选择“否”，请核实
    df = df_ex.merge(df_ph, on="SUBJID", how="left")
    df_ex_logic = df[(df["PHSPID"] == 3) & (df["PHYN"] == "是") & (df["EXSPID"] == 4) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="既往史描述3【女性是否在哺乳期】选择“是”，排除标准4【妊娠或哺乳期女性以及未来6个月计划怀孕的受试者（男性患者及育龄期女性患者在签署研究知情同意书起至检查结束后6个月内，必顼进行充分的避孕措施）】选择“否”，请核实",
            )
        )

    # 既往史描述2【是否试验前3个月内曾参加过其他临床研究】选择“是”，排除标准5【试验前3个月内曾参加过其他临床研究】选择“否”，请核实
    df = df_ex.merge(df_ph, on="SUBJID", how="left")
    df_ex_logic = df[(df["PHSPID"] == 2) & (df["PHYN"] == "是") & (df["EXSPID"] == 5) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_logic["SUBJID"], query="既往史描述2【是否试验前3个月内曾参加过其他临床研究】选择“是”，排除标准5【试验前3个月内曾参加过其他临床研究】选择“否”，请核实"))

    # 既往史描述2【是否试验前3个月内曾参加过其他临床研究】选择“否”，排除标准5【试验前3个月内曾参加过其他临床研究】选择“是”，请核实
    df = df_ex.merge(df_ph, on="SUBJID", how="left")
    df_ex_logic = df[(df["PHSPID"] == 2) & (df["PHYN"] == "否") & (df["EXSPID"] == 5) & (df["EXYN"] == "是")]
    if not df_ex_logic.empty:
        df_query_list.append(create_query_ex(variable="EXYN", label="结果", subjid=df_ex_logic["SUBJID"], query="既往史描述2【是否试验前3个月内曾参加过其他临床研究】选择“否”，排除标准5【试验前3个月内曾参加过其他临床研究】选择“是”，请核实"))

    # 既往史描述7【是否存在碘对比剂过敏史（增强）】或既往史描述8【是否存在哮喘病史（增强）】有选择“是”，排除标准7【不适宜使用碘对比剂进行增强扫描的高危人群，比如碘对比剂过敏、哮喘等】选择“否”，请核实
    df = df_ex.merge(df_ph[df_ph["PHSPID"] == 7], on="SUBJID", how="left").merge(df_ph[df_ph["PHSPID"] == 8], on="SUBJID", how="left")
    df_ex_logic = df[((df["PHYN_x"] == "是") | (df["PHYN_y"] == "是")) & (df["EXSPID"] == 7) & (df["EXYN"] == "否")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="既往史描述7【是否存在碘对比剂过敏史（增强）】或既往史描述8【是否存在哮喘病史（增强）】有选择“是”，排除标准7【不适宜使用碘对比剂进行增强扫描的高危人群，比如碘对比剂过敏、哮喘等】选择“否”，请核实",
            )
        )

    # 既往史描述7【是否存在碘对比剂过敏史（增强）】或既往史描述8【是否存在哮喘病史（增强）】均选择“否”，排除标准7【不适宜使用碘对比剂进行增强扫描的高危人群，比如碘对比剂过敏、哮喘等】选择“是”，请核实
    df = df_ex.merge(df_ph[df_ph["PHSPID"] == 7], on="SUBJID", how="left").merge(df_ph[df_ph["PHSPID"] == 8], on="SUBJID", how="left")
    df_ex_logic = df[(df["PHYN_x"] == "否") & (df["PHYN_y"] == "否") & (df["EXSPID"] == 7) & (df["EXYN"] == "是")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="既往史描述7【是否存在碘对比剂过敏史（增强）】或既往史描述8【是否存在哮喘病史（增强）】均选择“否”，排除标准7【不适宜使用碘对比剂进行增强扫描的高危人群，比如碘对比剂过敏、哮喘等】选择“是”，请核实",
            )
        )

    # 【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，排除标准7【不适宜使用碘对比剂进行增强扫描的高危人群，比如碘对比剂过敏、哮喘等】选择“不适用”，请核实
    df = df_ex.merge(df_ie, on="SUBJID", how="left")
    df_ex_logic = df[(df["ECTCYN"].notna()) & (df["ECTCYN"] != "不适用") & (df["EXSPID"] == 7) & (df["EXYN"] == "不适用")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，排除标准7【不适宜使用碘对比剂进行增强扫描的高危人群，比如碘对比剂过敏、哮喘等】选择“不适用”，请核实",
            )
        )

    # 【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，排除标准8【肾功能不全者（血肌酐＞ULN）】选择“不适用”，请核实
    df = df_ex.merge(df_ie, on="SUBJID", how="left")
    df_ex_logic = df[(df["ECTCYN"].notna()) & (df["ECTCYN"] != "不适用") & (df["EXSPID"] == 8) & (df["EXYN"] == "不适用")]
    if not df_ex_logic.empty:
        df_query_list.append(
            create_query_ex(
                variable="EXYN",
                label="结果",
                subjid=df_ex_logic["SUBJID"],
                query="【初拟增强检查经筛选后是否有变更】不为空且未选择“不适用”，排除标准8【肾功能不全者（血肌酐＞ULN）】选择“不适用”，请核实",
            )
        )

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()


def check_ex(df_ex: pd.DataFrame, df_info: pd.DataFrame, df_lb: pd.DataFrame, df_ph: pd.DataFrame, df_ie: pd.DataFrame) -> pd.DataFrame:
    df_ex = df_ex.dropna(how="all")

    df_query_list = []
    df_query_list.append(check_ex_missing(df_ex))
    df_query_list.append(check_ex_logic(df_ex, df_info, df_lb, df_ph, df_ie))
    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
