## Description
Data source of a CSV (Comma Separated Values) file.
 
 The file can reside in places including:
 <ul>
 <li> local file system
 <li> hdfs
 <li> http
 </ul></p>

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |
| schemaStr | Formatted schema | String | ✓ |  |
| fieldDelimiter | Field delimiter | String |  | "," |
| quoteChar | quote char | Character |  | "\"" |
| skipBlankLine | skipBlankLine | Boolean |  | true |
| rowDelimiter | Row delimiter | String |  | "\n" |
| ignoreFirstLine | Whether to ignore first line of csv file. | Boolean |  | false |
| lenient | lenient | Boolean |  | false |

## Script Example

### Code

### Code


```python
filePath = 'http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceStreamOp()\
    .setFilePath(filePath)\
    .setSchemaStr(schema)\
    .setFieldDelimiter(",")
csvSource.print()
StreamOperator.execute()
```

### Results

```python
sepal_length	sepal_width	petal_length	petal_width	category
1	5.5	2.4	3.8	1.1	Iris-versicolor
2	6.1	2.6	5.6	1.4	Iris-virginica
3	6.0	2.2	4.0	1.0	Iris-versicolor
4	5.5	2.4	3.7	1.0	Iris-versicolor
5	4.6	3.1	1.5	0.2	Iris-setosa
```


