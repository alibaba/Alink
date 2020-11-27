## Description
StreamOperator to sink data a file in plain text lines.

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

data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).select("category")

sink = TextSinkStreamOp().setFilePath('/tmp/text.csv').setOverwriteSink(True)
data.link(sink)
StreamOperator.execute()
```
