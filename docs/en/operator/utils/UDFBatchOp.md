## Description

 outputColNames CAN NOT have the same colName with reservedColName except the selectedColName.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example

### Code

```
class PlusOne(object):
    def eval(self, x):
        return x + 1
    pass

source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

udfOp = UDFBatchOp() \
    .setFunc(PlusOne()) \
    .setResultType("DOUBLE") \
    .setSelectedCols(['sepal_length']) \
    .setOutputCol('sepal_length_t') \
    .setReservedCols(['sepal_width'])
res = udfOp.linkFrom(source)

res.firstN(10).print()
```


### Results

```
   sepal_length_t  sepal_width
0             6.0          3.2
1             7.6          3.0
2             6.4          3.9
3             6.0          2.3
4             6.1          3.5
5             6.0          2.0
6             6.5          3.5
7             7.2          3.4
8             6.6          2.7
9             7.8          2.8
```

