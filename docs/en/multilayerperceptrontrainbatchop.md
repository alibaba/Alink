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


## Script Example
#### Code
```python
    mlpc = MultilayerPerceptronClassifier() \
        .setVectorCol("bitmap") \
        .setLabelCol("label") \
        .setLayers([628, 100, 100]) \
        .setMaxIter(100) \
        .setPredictionCol("pred_label") \
        .setPredictionDetailCol("pred_detail")

    # mlpc.fit(batch_data.bo_mnist)
    pipeline = Pipeline().add(mlpc)
    model = pipeline.fit(batch_data.bo_mnist)
    model.save('/tmp/mlpc.csv')
    BatchOperator.execute()
```

#### Results

```
-1,"{""schema"":[""model_id BIGINT,model_info VARCHAR,label_value BIGINT""],""param"":[""{\""vectorCol\"":\""\\\""bitmap\\\""\"",\""maxIter\"":\""100\"",\""layers\"":\""[628,100,100]\"",\""labelCol\"":\""\\\""label\\\""\"",\""predictionCol\"":\""\\\""pred_label\\\""\"",\""predictionDetailCol\"":\""\\\""pred_detail\\\""\""}""],""clazz"":[""com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassificationModel""]}"
0,"0^{""vectorCol"":""\""bitmap\"""",""isVectorInput"":""true"",""layers"":""[628,100,100]"",""featureCols"":null}^"
......
```

