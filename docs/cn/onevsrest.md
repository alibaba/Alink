## 功能介绍

本组件用One VS Rest策略进行多分类。

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numClass | 类别数 | 多分类的类别数，必选 | Integer | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码
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
