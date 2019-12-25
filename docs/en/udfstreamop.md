## Description

 outputColNames CAN NOT have the same colName with keepOriginColName except the selectedColName.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example

### Code

```
source = CsvSourceStreamOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

udfOp = UDFStreamOp() \
    .setFunc(lambda x: x + 1) \
    .setResultType("DOUBLE") \
    .setSelectedCols(['sepal_length']) \
    .setOutputCol('sepal_length_t') \
    .setReservedCols(['sepal_width'])
res = udfOp.linkFrom(source)

res.print()
StreamOperator.execute()
```


### Results

```
     sepal_length_t	sepal_width
1               6.9         3.2
2               6.4         3.7
3               7.9         3.1
4               6.5         2.5
5               6.4         3.4
...             ...         ...
96              8.9         3.8
97              6.2         2.7
98              7.4         2.7
99              7.8         3.0
100	            6.7         2.5
```

