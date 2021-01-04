## Description
Select execute select statement for each row.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| clause | Operation clause. | String | ✓ |  |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
### Code

```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
select = Select().setClause("category as label")
select.transform(data).print()
```

#### Results
```
           label
 Iris-versicolor
  Iris-virginica
  Iris-virginica
 Iris-versicolor
  Iris-virginica
             ...
  Iris-virginica
 Iris-versicolor
     Iris-setosa
  Iris-virginica
 Iris-versicolor
```
