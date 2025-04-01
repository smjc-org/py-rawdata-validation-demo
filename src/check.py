from datetime import datetime
from pathlib import Path
import pandas as pd

from check_info import check_info
from check_ph import check_ph
from check_lb import check_lb
from check_in import check_in
from check_ex import check_ex
from check_ie import check_ie
from check_ct import check_ct
from check_ctpar import check_ctpar
from check_cfe import check_cfe
from check_pce import check_pce
from check_pse import check_pse
from check_iqe import check_iqe
from check_cm import check_cm
from check_cn import check_cn
from check_ae import check_ae
from check_sae import check_sae
from check_ed import check_ed
from check_pd import check_pd
from check_ds import check_ds

# 设置路径
path_dir_dm_check = Path.cwd().parents[0]
path_dir_dm = Path.cwd().parents[1]
path_dir_project = Path.cwd().parents[2]
path_dir_statistics = path_dir_project / "04 统计分析"
path_dir_rawdata = path_dir_statistics / "02 原始数据"
path_excel_rawdata = path_dir_rawdata / "数据库_V1.0_20250303.xlsx"
path_excel_query = path_dir_dm_check / "03 output" / f"数据核查-质疑清单-{datetime.now().strftime("%Y%m%d-%H%M%S")}.xlsx"
print(path_excel_rawdata)

# 读取 excel 文件
df_info = pd.read_excel(path_excel_rawdata, sheet_name="基本信息(INFO)", header=1)
df_ph = pd.read_excel(path_excel_rawdata, sheet_name="既往史及个人史(PH)", header=1)
df_lb = pd.read_excel(path_excel_rawdata, sheet_name="实验室检查(LB)", header=1)
df_in = pd.read_excel(path_excel_rawdata, sheet_name="入选标准(IN)", header=1)
df_ex = pd.read_excel(path_excel_rawdata, sheet_name="排除标准(EX)", header=1)
df_ie = pd.read_excel(path_excel_rawdata, sheet_name="入选排除筛选(IE)", header=1)
df_ct = pd.read_excel(path_excel_rawdata, sheet_name="CT扫描(CT)", header=1)
df_ctpar = pd.read_excel(path_excel_rawdata, sheet_name="CT扫描-重建参数(CTPAR)", header=1)
df_cfe = pd.read_excel(path_excel_rawdata, sheet_name="常用功能评价(CFE)", header=1)
df_pce = pd.read_excel(path_excel_rawdata, sheet_name="器械使用便捷性评价(PCE)", header=1)
df_pse = pd.read_excel(path_excel_rawdata, sheet_name="整机功能性及稳定性满意度评价(PSE)", header=1)
df_iqe = pd.read_excel(path_excel_rawdata, sheet_name="图像质量评价(IQE)", header=1)
df_cm = pd.read_excel(path_excel_rawdata, sheet_name="合并用药(CM)", header=1)
df_cn = pd.read_excel(path_excel_rawdata, sheet_name="伴随治疗(CN)", header=1)
df_ae = pd.read_excel(path_excel_rawdata, sheet_name="不良事件(AE)", header=1)
df_sae = pd.read_excel(path_excel_rawdata, sheet_name="严重不良事件(SAE)", header=1)
df_ed = pd.read_excel(path_excel_rawdata, sheet_name="器械缺陷记录(ED)", header=1)
df_pd = pd.read_excel(path_excel_rawdata, sheet_name="方案偏离记录(PD)", header=1)
df_ds = pd.read_excel(path_excel_rawdata, sheet_name="试验总结(DS)", header=1, parse_dates=["DSENDAT"])

# 核查数据
query_list = []
query_list.append(check_info(df_info, df_ct))
query_list.append(check_ph(df_ph, df_info, df_ie))
query_list.append(check_lb(df_lb, df_info, df_ct, df_ie))
query_list.append(check_in(df_in, df_info, df_lb))
query_list.append(check_ex(df_ex, df_info, df_lb, df_ph, df_ie))
query_list.append(check_ie(df_ie, df_in, df_ex, df_info, df_ct))
query_list.append(check_ct(df_ct, df_ie))
query_list.append(check_ctpar(df_ctpar))
query_list.append(check_cfe(df_cfe, df_ct))
query_list.append(check_pce(df_pce, df_ct))
query_list.append(check_pse(df_pse, df_ct))
query_list.append(check_iqe(df_iqe))
query_list.append(check_cm(df_cm, df_ae, df_info))
query_list.append(check_cn(df_cn, df_info))
query_list.append(check_ae(df_ae, df_ds, df_ct))
query_list.append(check_sae(df_sae, df_info))
query_list.append(check_ed(df_ed, df_ct, df_ds))
query_list.append(check_pd(df_pd, df_info, df_ds))
query_list.append(check_ds(df_ds))

df_query = pd.concat(query_list, ignore_index=True)
print(f"质疑数量：{len(df_query)}")

# 输出 Excel 文件
df_query.to_excel(path_excel_query, index=False)
