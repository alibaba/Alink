## Description

 outputColNames CAN NOT have the same colName with reservedColName except the selectedColName.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCols | Names of the output columns | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example

### Code

```
class SplitOp(object):
    def eval(self, *args):
        for index, x in enumerate(args):
            yield index, x
    pass

source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
udtfOp = UDTFBatchOp() \
    .setFunc(SplitOp()) \
    .setResultTypes(["LONG", "DOUBLE"]) \
    .setSelectedCols(['sepal_length', 'sepal_width']) \
    .setOutputCols(['index', 'x'])
udtf_res = udtfOp.linkFrom(source)

udtf_res.firstN(10).print()
```


### Results

```
   index    x
0      0  5.1
1      1  3.5
2      0  5.0
3      1  2.0
4      0  5.0
5      1  3.2
6      0  6.6
7      0  5.1
8      1  3.7
9      0  6.4
```


