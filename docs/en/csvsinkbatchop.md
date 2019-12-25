## Description
Sink to local or HDFS files in CSV format.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path | String | ✓ |  |
| fieldDelimiter | Field delimiter | String |  | "," |
| rowDelimiter | Row delimiter | String |  | "\n" |
| quoteChar | quote char | Character |  | "\"" |
| overwriteSink | Whether to overwrite existing data. | Boolean |  | false |
| numFiles | Number of files | Integer |  | 1 |


## Script Example

#### batch sink

```python
filePath = 'https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceBatchOp()\
    .setFilePath(filePath)\
    .setSchemaStr(schema)\
    .setFieldDelimiter(",")
csvSink = CsvSinkBatchOp()\
    .setFilePath('~/csv_test.txt')

csvSource.link(csvSink)

BatchOperator.execute()
```


#### stream sink

```python
filePath = 'https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceStreamOp()\
    .setFilePath(filePath)\
    .setSchemaStr(schema)\
    .setFieldDelimiter(",")
csvSink = CsvSinkStreamOp()\
    .setFilePath('~/csv_test_s.txt')

csvSource.link(csvSink)

StreamOperator.execute()
```


