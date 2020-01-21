## Description
Make prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
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

classifier = MultilayerPerceptronClassifier()\
            .setFeatureCols(Iris.getFeatureColNames())\
            .setLabelCol(Iris.getLabelColName())\
            .setLayers([4, 5, 3])\
            .setMaxIter(100)\
            .setPredictionCol("pred_label")\
            .setPredictionDetailCol("pred_detail")

classifier.fit(data).transform(data).print();
```

#### Results

```
6.3000|3.3000|6.0000|2.5000|Iris-virginica|Iris-virginica|{"Iris-virginica":0.9433614954932688,"Iris-versicolor":0.056638504506731226,"Iris-setosa":3.008568854761749E-175}
5.6000|2.8000|4.9000|2.0000|Iris-virginica|Iris-virginica|{"Iris-virginica":0.9433614954932688,"Iris-versicolor":0.056638504506731226,"Iris-setosa":3.008568854761749E-175}
5.0000|3.3000|1.4000|0.2000|Iris-setosa|Iris-setosa|{"Iris-virginica":8.4E-323,"Iris-versicolor":4.0138401486628416E-173,"Iris-setosa":1.0}
5.8000|2.7000|5.1000|1.9000|Iris-virginica|Iris-virginica|{"Iris-virginica":0.9433614954932688,"Iris-versicolor":0.056638504506731226,"Iris-setosa":3.008568854761749E-175}
7.0000|3.2000|4.7000|1.4000|Iris-versicolor|Iris-versicolor|{"Iris-virginica":5.31328185381337E-80,"Iris-versicolor":1.0,"Iris-setosa":6.407249280059006E-44}
```

