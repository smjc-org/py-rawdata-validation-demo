# py-rawdata-validation

这是一个使用 Python 进行数据核查的示例。

## 运行环境准备

- [Python~=3.13](https://www.python.org/downloads/)
- [pandas[excel]==2.2.3](https://github.com/pandas-dev/pandas)

## 程序启动入口

> [!NOTE]
>
> 建议在运行程序前建立虚拟环境。

[check.py](./src/check.py) 是程序的启动入口，这个文件包含以下内容：

1. 设置路径

   | 变量名                | 含义             | 是否必须 |
   | --------------------- | ---------------- | -------- |
   | `path_dir_dm_check`   | 数据核查目录     | ✖️       |
   | `path_dir_dm`         | 数据管理目录     | ✖️       |
   | `path_dir_project`    | 项目目录         | ✖️       |
   | `path_dir_statistics` | 统计分析目录     | ✖️       |
   | `path_dir_rawdata`    | 待核查数据目录   | ✖️       |
   | `path_excel_rawdata`  | 待核查数据文件   | ✔️       |
   | `path_excel_query`    | 质疑清单输出文件 | ✔️       |

2. 读取 Excel 文件

   使用 `pandas.read_excel` 函数读取 Excel 中的数据。

   - `parse_dates` 可用于指定部分没有被正确识别的日期型变量
   - `keep_default_na` 可用于指定是否将部分 `N/A`，`NA`，... 字符串识别为缺失值 `NaN`

3. 调用子模块

   `query_list` 是一个 `DataFrame` 对象列表，用于保存质疑列表。`DataFrame` 对象的具体数据结构由 [query.py](./src/query.py) 中的类 `Query` 定义如下：

   | 变量       | 含义       |
   | ---------- | ---------- |
   | `vistoid`  | 访视编号   |
   | `formoid`  | 表单标签   |
   | `formnm`   | 表单名称   |
   | `variable` | 变量名     |
   | `label`    | 变量标签   |
   | `subjid`   | 受试者编号 |
   | `query`    | 质疑文本   |

   > [!IMPORTANT]
   >
   > 根据数据核查计划（DVP）的差异，上述 `Query` 定义的结构应当做适当改动。

   每一个子模块都以 `check_` 开头，分别是对应每一个表单的 Python 核查代码，其中的函数 `check_xxx` 返回一个 `DataFrame` 对象列表，保存了某个表单的质疑列表。

   使用 `pandas.concat` 函数将 `query_list` 合并为一个 `DateFrame` 对象。

4. 输出质疑清单

   使用 `pandas.DataFrame.to_excel` 函数将 `DateFrame` 对象输出为 Excel 文件。

## 一些细节

### 公共字段

对于公共字段（`方案编号`，`研究中心`，`受试者编号`，...）的核查，应当提取成单独的子模块，避免在每一个子模块中重复相同代码。

本项目中的公共字段核查在子模块 [check_common.py](./src/check_common.py) 中定义。

在其他子模块中调用：

```py
df_query_list.append(check_common(df=df_ct, vistoid=vistoid, formoid=formoid, formnm=formnm))
```

### 过滤数据

对于一些 `Event` 类型的数据集，某些受试者可能不存在相应数据，其对应的 `YN` 变量为 `否`，数据核查前应当先过滤这部分受试者：

```py
df_ae = df_ae[df_ae["AEYN"] == "是"]
if df_ae.empty:
    return pd.DataFrame()
```

### 移除空行

某些空行可能是为了方便查看数据而特意保留的，数据核查前应当使用 `pandas.DataFrame.dropna` 函数移除这些空行：

```py
df_ph = df_ph.dropna(how="all")
```

### 偏函数

`Query` 类中的 `create_query` 函数需要被频繁地调用，但在同一子模块中，它的部分参数是不会变化的，此时，可使用 `functools.partial` 简化函数的签名：

```py
create_query_ct = partial(create_query, vistoid=vistoid, formoid=formoid, formnm=formnm)
```

然后在子模块中使用新的函数名调用：

```py
create_query_ct(variable="CTDAT", label="检查日期", subjid=df_ct_ctdat_missing["SUBJID"], query="此字段必填")
```
