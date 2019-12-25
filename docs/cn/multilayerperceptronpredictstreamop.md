# 多层感知机预测

## 功能介绍
基于多层感知机模型，进行分类预测。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

classifier = MultilayerPerceptronClassifier()\
            .setFeatureCols(Iris.getFeatureColNames())\
            .setLabelCol(Iris.getLabelColName())\
            .setLayers([4, 5, 3])\
            .setMaxIter(100)\
            .setPredictionCol("pred_label")\
            .setPredictionDetailCol("pred_detail")

classifier.fit(data).transform(Iris.getStreamData()).print();
```

#### 脚本运行结果

```
6.3000|3.3000|6.0000|2.5000|Iris-virginica|Iris-virginica|{"Iris-virginica":0.9433614954932688,"Iris-versicolor":0.056638504506731226,"Iris-setosa":3.008568854761749E-175}
5.6000|2.8000|4.9000|2.0000|Iris-virginica|Iris-virginica|{"Iris-virginica":0.9433614954932688,"Iris-versicolor":0.056638504506731226,"Iris-setosa":3.008568854761749E-175}
5.0000|3.3000|1.4000|0.2000|Iris-setosa|Iris-setosa|{"Iris-virginica":8.4E-323,"Iris-versicolor":4.0138401486628416E-173,"Iris-setosa":1.0}
5.8000|2.7000|5.1000|1.9000|Iris-virginica|Iris-virginica|{"Iris-virginica":0.9433614954932688,"Iris-versicolor":0.056638504506731226,"Iris-setosa":3.008568854761749E-175}
7.0000|3.2000|4.7000|1.4000|Iris-versicolor|Iris-versicolor|{"Iris-virginica":5.31328185381337E-80,"Iris-versicolor":1.0,"Iris-setosa":6.407249280059006E-44}
```
