from dataclasses import dataclass

import pandas as pd


@dataclass
class Query:
    vistoid: str
    formoid: str
    formnm: str
    variable: str
    label: str
    subjid: pd.Series
    query: str

    def to_dictionary(self) -> dict[str, str | pd.Series]:
        return {
            "访视编号": self.vistoid,
            "表单标签": self.formoid,
            "表单名称": self.formnm,
            "变量名": self.variable,
            "变量标签": self.label,
            "受试者编号": self.subjid,
            "质疑文本": self.query,
        }


def create_query(vistoid: str, formoid: str, formnm: str, variable: str, label: str, subjid: pd.Series, query: str) -> pd.DataFrame:
    return pd.DataFrame(
        Query(
            vistoid=vistoid,
            formoid=formoid,
            formnm=formnm,
            variable=variable,
            label=label,
            subjid=subjid.unique(),
            query=query,
        ).to_dictionary()
    )
