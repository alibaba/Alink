## Description
StreamOperator to sink data in libsvm format.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path | String | ✓ |  |
| overwriteSink | Whether to overwrite existing data. | Boolean |  | false |
| vectorCol | Name of a vector column | String | ✓ |  |
| labelCol | Name of the label column in the input table | String | ✓ |  |


## Script Example
#### Script
```
URL = "http://alink-testdata.cn-hangzhou.oss.aliyun-inc.com/csv/iris_vec.csv";
SCHEMA_STR = "features string, label double"
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

LibSvmSinkStreamOp().setFilePath('/tmp/libsvm.csv') \
    .setLabelCol("label").setVectorCol("features").setOverwriteSink(True).linkFrom(data)
StreamOperator.execute()

```

