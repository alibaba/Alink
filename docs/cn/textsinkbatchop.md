## 功能介绍

按行写出到文件

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |
| numFiles | 文件数目 | 文件数目 | Integer |  | 1 |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 运行脚本
```
URL = "http://alink-testdata.cn-hangzhou.oss.aliyun-inc.com/csv/iris_vec.csv";
SCHEMA_STR = "features string, label double"
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

sink = TextSinkBatchOp().setFilePath('/tmp/text.csv')
data.select('features').link(sink)
BatchOperator.execute()

```
