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
data = np.array([
    ["-0.6264538 0.1836433"],
    ["-0.8356286 1.5952808"],
    ["0.3295078 -0.8204684"],
    ["0.4874291 0.7383247"],
    ["0.5757814 -0.3053884"],
    ["1.5117812 0.3898432"],
    ["-0.6212406 -2.2146999"],
    ["11.1249309 9.9550664"],
    ["9.9838097 10.9438362"],
    ["10.8212212 10.5939013"],
    ["10.9189774 10.7821363"],
    ["10.0745650 8.0106483"],
    ["10.6198257 9.9438713"],
    ["9.8442045 8.5292476"],
    ["9.5218499 10.4179416"],
])

df_data = pd.DataFrame({
    "features": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='features string', op_type='batch')

gmm = GmmTrainBatchOp() \
    .setVectorCol("features") \
    .setTol(0.)

model = gmm.linkFrom(data)

predictor = GmmPredictBatchOp() \
    .setPredictionCol("cluster_id") \
    .setVectorCol("features") \
    .setPredictionDetailCol("cluster_detail")

predictor.linkFrom(model, data).print()
```

#### Results

```
                 features  cluster_id              cluster_detail
0    -0.6264538 0.1836433           0   1.0 4.275273913994647E-92
1    -0.8356286 1.5952808           0  1.0 1.0260377730322135E-92
2    0.3295078 -0.8204684           0  1.0 1.0970173367582936E-80
3     0.4874291 0.7383247           0    1.0 3.30217313232611E-75
4    0.5757814 -0.3053884           0   1.0 3.163811360527691E-76
5     1.5117812 0.3898432           0  1.0 2.1018052308786076E-62
6   -0.6212406 -2.2146999           0   1.0 6.772270268625197E-97
7    11.1249309 9.9550664           1  3.1567838012477083E-56 1.0
8    9.9838097 10.9438362           1  1.9024447346702333E-51 1.0
9   10.8212212 10.5939013           1  2.8009730987296404E-56 1.0
10  10.9189774 10.7821363           1  1.7209132744891575E-57 1.0
11   10.0745650 8.0106483           1   2.864269663513225E-43 1.0
12   10.6198257 9.9438713           1    5.77327399194046E-53 1.0
13    9.8442045 8.5292476           1  2.5273123050926845E-43 1.0
14   9.5218499 10.4179416           1  1.7314580596765865E-46 1.0
```

