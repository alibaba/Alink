## Description
Order the batch operator.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| fetch | Number of records to fetch | Integer |  |  |
| limit | Number of records limited | Integer |  |  |
| offset | Offset when fetching records | Integer |  |  |
| order | asc or desc | String |  | "asc" |
| clause | Operation clause. | String | ✓ |  |


## Script Example
#### Code

```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(OrderByBatchOp().setLimit(10).setClause("sepal_length"))
```

