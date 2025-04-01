import pandas as pd

from query import Query


# 公共字段的空值核查
def check_common(df: pd.DataFrame, vistoid: str, formoid: str, formnm: str) -> pd.DataFrame:
    df_query_list = []

    common_columns = ["SITENM", "SITEID", "PSTUDYNM", "PSTUDYID", "SUBJID", "SUBJINIT", "SUBJSTA", "VISIT", "VISTOID", "FORMNM", "FORMOID", "PISIGN", "SIGNDAT"]
    common_labels = [
        "研究中心",
        "中心编号",
        "项目名称",
        "方案编号",
        "受试者筛选号",
        "受试者姓名缩写",
        "受试者状态",
        "访视名称",
        "访视OID",
        "表单名称",
        "表单OID",
        "研究者签名",
        "签名日期",
    ]

    for column, label in zip(common_columns, common_labels):
        if column in df.columns:
            df_missing = df[df[column].isna()]
            if not df_missing.empty:
                df_query_list.append(
                    pd.DataFrame(
                        Query(
                            vistoid=vistoid,
                            formoid=formoid,
                            formnm=formnm,
                            variable=column,
                            label=label,
                            subjid=df_missing["SUBJID"].unique(),
                            query="此字段必填",
                        ).to_dictionary()
                    )
                )

    return pd.concat(df_query_list) if df_query_list else pd.DataFrame()
