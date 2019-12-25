## Description
Gaussian Mixture prediction based on the model fitted by GmmTrainBatchOp.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String | ✓ |  |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example
#### Code
```python
    URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
    SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
    so_iris = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
    bo_iris = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

    va = VectorAssembler() \
        .setSelectedCols(['sepal_length', 'sepal_width', 'petal_length', 'petal_width']) \
        .setOutputCol('features')

    gmm = GaussianMixture() \
        .setVectorCol('features') \
        .setK(3) \
        .setMaxIter(100) \
        .setPredictionCol('cluster_id') \
        .setPredictionDetailCol('cluster_detail')

    pipeline = Pipeline().add(va).add(gmm)
    model = pipeline.fit(bo_iris)
    model.transform(so_iris).link(CsvSinkStreamOp().setFilePath('/tmp/gmm_pred.csv'))
    StreamOperator.execute()
```

#### Results

```
5.0,3.2,1.2,0.2,Iris-setosa,5.0 3.2 1.2 0.2,0,1.0 1.0046812472553649E-25 4.0513728391732534E-35
6.6,3.0,4.4,1.4,Iris-versicolor,6.6 3.0 4.4 1.4,1,6.378045617038367E-82 0.9998755827709217 1.244172290783167E-4
5.4,3.9,1.3,0.4,Iris-setosa,5.4 3.9 1.3 0.4,0,1.0 2.549038258209056E-36 4.426016725123766E-44
5.0,2.3,3.3,1.0,Iris-versicolor,5.0 2.3 3.3 1.0,1,4.222741118453436E-36 0.9999377852049277 6.22147950723104E-5
5.5,3.5,1.3,0.2,Iris-setosa,5.5 3.5 1.3 0.2,0,1.0 9.30170187388185E-33 8.269822554170461E-42

```

