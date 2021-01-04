## 功能介绍

按行写出到文件

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |
| numFiles | 文件数目 | 文件数目 | Integer |  | 1 |

## 脚本示例
#### 运行脚本
```
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"

data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).select("category")

sink = TextSinkStreamOp().setFilePath('/tmp/text.csv').setOverwriteSink(True)
data.link(sink)
StreamOperator.execute()
```
