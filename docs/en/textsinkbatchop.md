## Description
Sink data to files in text lines.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path | String | ✓ |  |
| overwriteSink | Whether to overwrite existing data. | Boolean |  | false |
| numFiles | Number of files | Integer |  | 1 |


## Script Example
#### Script
```
URL = "http://alink-testdata.cn-hangzhou.oss.aliyun-inc.com/csv/iris_vec.csv";
SCHEMA_STR = "features string, label double"
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

sink = TextSinkBatchOp().setFilePath('/tmp/text.csv')
data.select('features').link(sink)
BatchOperator.execute()

```

