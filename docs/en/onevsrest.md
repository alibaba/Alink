## Description
Reduction of Multiclass Classification to Binary Classification.
 Performs reduction using one against all strategy.
 For a multiclass classification with k classes, train k models (one per class).
 Each example is scored against all k models and the model with highest score
 is picked to label the example.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| numClass | num class of multi class train. | Integer | ✓ |  |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example
#### Code
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

lr = LogisticRegression() \
    .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
    .setLabelCol("category") \
    .setMaxIter(100)

oneVsRest = OneVsRest().setClassifier(lr).setNumClass(3)
model = oneVsRest.fit(data)
model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail")
model.transform(data).print()
```

