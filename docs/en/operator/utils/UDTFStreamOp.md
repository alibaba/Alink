## Description

 outputColNames CAN NOT have the same colName with keepOriginColName except the selectedColName.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCols | Names of the output columns | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example

### Code

```
source = CsvSourceStreamOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

udtfOp = UDTFStreamOp()\
    .setFunc(lambda x, y: [ (yield x + 1 + i, y + 2 + i) for i in range(3) ])\
    .setResultTypes(["DOUBLE", "DOUBLE"])\
    .setSelectedCols(['sepal_length', 'sepal_width'])\
    .setOutputCols(['index', 'x'])\
    .setReservedCols(['sepal_length', 'sepal_width'])
res = udtfOp.linkFrom(source)
res.print()

StreamOperator.execute()
```


### Results

```
  sepal_length sepal_width index    x
1          5.2         4.1   6.2  6.1
2          5.2         4.1   7.2  7.1
3          5.2         4.1   8.2  8.1
4          5.5         2.6   6.5  4.6
5          5.5         2.6   7.5  5.6
...        ...         ...   ...  ...
96         5.7         4.4   7.7  7.4
97         5.7         4.4   8.7  8.4
98         6.5         3.0   7.5  5.0
99         6.5         3.0   8.5  6.0
100        6.5         3.0   9.5  7.0
```


