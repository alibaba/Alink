## Description
MultilayerPerceptronClassifier is a neural network based multi-class classifier.
 Valina neural network with all dense layers are used, the output layer is a softmax layer.
 Number of inputs has to be equal to the size of feature vectors.
 Number of outputs has to be equal to the total number of labels.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| layers | Size of each neural network layers. | int[] | ✓ |  |
| blockSize | Size for stacking training samples, the default value is 64. | Integer |  | 64 |
| initialWeights | Initial weights. | DenseVector |  | null |
| vectorCol | Name of a vector column | String |  | null |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| epsilon | Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06 | Double |  | 1.0E-6 |
| l1 | the L1-regularized parameter. | Double |  | 0.0 |
| l2 | the L2-regularized parameter. | Double |  | 0.0 |
| vectorCol | Name of a vector column | String |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example
#### Code
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

#### Results
```
   sepal_length  sepal_width  ...       pred_label                                        pred_detail
0           5.1          3.5  ...      Iris-setosa  {"Iris-versicolor":4.847903295060146E-12,"Iris...
1           5.0          2.0  ...  Iris-versicolor  {"Iris-versicolor":0.5316800097281505,"Iris-vi...
2           5.1          3.7  ...      Iris-setosa  {"Iris-versicolor":9.36626517454266E-10,"Iris-...
3           6.4          2.8  ...   Iris-virginica  {"Iris-versicolor":0.19480380926794844,"Iris-v...
```
