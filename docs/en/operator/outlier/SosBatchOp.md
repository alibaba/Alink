## Description
Stochastic Outlier Selection algorithm.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| perplexity | Perplexity | Double |  | 4.0 |
| vectorCol | Name of a vector column | String | ✓ |  |
| predictionCol | Column name of prediction. | String | ✓ |  |

## Script Example
### Code
```python
data = np.array([
  ["0.0,0.0"],
  ["0.0,1.0"],
  ["1.0,0.0"],
  ["1.0,1.0"],
  ["5.0,5.0"],
])

df_data = pd.DataFrame({
    "features": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='features string', op_type='batch')
sos = SosBatchOp().setVectorCol("features").setPredictionCol("outlier_score").setPerplexity(3.0)

output = sos.linkFrom(data)
output.print()
```

### Results

features|outlier_score
--------|-------------
1.0,1.0|0.12396819612216292
0.0,0.0|0.27815186043725715
0.0,1.0|0.24136320497783578
1.0,0.0|0.24136320497783578
5.0,5.0|0.9998106220648153

