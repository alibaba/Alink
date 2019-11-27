## Description
Calculate the evaluation data for regression. The evaluation metrics are: SST: Sum of Squared for Total SSE: Sum of
 Squares for Error SSR: Sum of Squares for Regression R^2: Coefficient of Determination R: Multiple CorrelationBak
 Coeffient MSE: Mean Squared Error RMSE: Root Mean Squared Error SAE/SAD: Sum of Absolute Error/Difference MAE/MAD:
 Mean Absolute Error/Difference MAPE: Mean Absolute Percentage Error

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| predictionCol | Column name of prediction. | String | ✓ |  |


## Script Example
#### Code

```
import numpy as np
import pandas as pd
data = np.array([
    [0, 0],
    [8, 8],
    [1, 2],
    [9, 10],
    [3, 1],
    [10, 7]
])
df = pd.DataFrame({"pred": data[:, 0], "label": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='pred int, label int')

metrics = EvalRegressionBatchOp().setPredictionCol("pred").setLabelCol("label").linkFrom(inOp).collectMetrics()

print("Total Samples Number:", metrics.getCount())
print("SSE:", metrics.getSse())
print("SAE:", metrics.getSae())
print("RMSE:", metrics.getRmse())
print("R2:", metrics.getR2())
```

#### Results
```
Total Samples Number: 6.0
SSE: 15.0
SAE: 7.0
RMSE: 1.5811388300841898
R2: 0.8282442748091603
```



