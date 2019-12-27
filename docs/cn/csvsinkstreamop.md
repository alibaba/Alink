## 功能介绍
写CSV文件

## 参数说明


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |
| rowDelimiter | 行分隔符 | 行分隔符 | String |  | "\n" |
| quoteChar | 引号字符 | 引号字符 | Character |  | "\"" |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |
| numFiles | 文件数目 | 文件数目 | Integer |  | 1 |<!-- This is the end of auto-generated parameter info -->


## 脚本示例

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

