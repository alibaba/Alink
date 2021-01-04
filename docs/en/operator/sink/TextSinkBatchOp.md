## Description
Sink data to files in text lines.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |
| overwriteSink | Whether to overwrite existing data. | Boolean |  | false |
| numFiles | Number of files | Integer |  | 1 |

## Script Example
#### Script
```
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"

data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).select("category")

sink = TextSinkBatchOp().setFilePath('/tmp/text.csv')
data.link(sink)
BatchOperator.execute()
```
