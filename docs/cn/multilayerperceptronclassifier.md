## 功能介绍
多层感知机多分类模型

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| layers | 神经网络层大小 | 神经网络层大小 | int[] | ✓ |  |
| blockSize | 数据分块大小，默认值64 | 数据分块大小，默认值64 | Integer |  | 64 |
| initialWeights | 初始权重值 | 初始权重值 | DenseVector |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |
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

mlpc = MultilayerPerceptronClassifier() \
    .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
    .setLabelCol("category") \
    .setLayers([4, 5, 3]) \
    .setMaxIter(20) \
    .setPredictionCol("pred_label") \
    .setPredictionDetailCol("pred_detail")

mlpc.fit(data).transform(data).firstN(4).print()
```

#### 脚本运行结果
```
   sepal_length  sepal_width  ...       pred_label                                        pred_detail
0           5.1          3.5  ...      Iris-setosa  {"Iris-versicolor":4.847903295060146E-12,"Iris...
1           5.0          2.0  ...  Iris-versicolor  {"Iris-versicolor":0.5316800097281505,"Iris-vi...
2           5.1          3.7  ...      Iris-setosa  {"Iris-versicolor":9.36626517454266E-10,"Iris-...
3           6.4          2.8  ...   Iris-virginica  {"Iris-versicolor":0.19480380926794844,"Iris-v...
```