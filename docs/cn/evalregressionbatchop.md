## 功能介绍
回归评估是对回归算法的预测结果进行效果评估，支持下列评估指标。

#### SST	 总平方和(Sum of Squared for Total)
<div align=center><img src="http://latex.codecogs.com/gif.latex?SST=\sum_{i=1}^{N}(y_i-\bar{y})^2" ></div>

#### SSE	 误差平方和(Sum of Squares for Error)
<div align=center><img src="http://latex.codecogs.com/gif.latex?SSE=\sum_{i=1}^{N}(y_i-f_i)^2" ></div>

#### SSR	 回归平方和(Sum of Squares for Regression)
<div align=center><img src="http://latex.codecogs.com/gif.latex?SSR=\sum_{i=1}^{N}(f_i-\bar{y})^2" ></div>

#### R^2	判定系数(Coefficient of Determination)
<div align=center><img src="http://latex.codecogs.com/gif.latex?R^2=1-\dfrac{SSE}{SST}" ></div>

#### R	 多重相关系数(Multiple Correlation Coeffient)
<div align=center><img src="http://latex.codecogs.com/gif.latex?R=\sqrt{R^2}" ></div>

#### MSE 均方误差(Mean Squared Error)
<div align=center><img src="http://latex.codecogs.com/gif.latex?MSE=\dfrac{1}{N}\sum_{i=1}^{N}(f_i-y_i)^2" ></div>

#### RMSE	均方根误差(Root Mean Squared Error)
<div align=center><img src="http://latex.codecogs.com/gif.latex?RMSE=\sqrt{MSE}" ></div>

#### SAE/SAD 绝对误差(Sum of Absolute Error/Difference)
<div align=center><img src="http://latex.codecogs.com/gif.latex?SAE=\sum_{i=1}^{N}|f_i-y_i|" ></div>

#### MAE/MAD 平均绝对误差(Mean Absolute Error/Difference)
<div align=center><img src="http://latex.codecogs.com/gif.latex?MAE=\dfrac{1}{N}\sum_{i=1}^{N}|f_i-y_i|" ></div>

#### MAPE 	平均绝对百分误差(Mean Absolute Percentage Error)
<div align=center><img src="http://latex.codecogs.com/gif.latex?MAPE=\dfrac{100}{N}\sum_{i=1}^{N}|\dfrac{f_i-y_i}{y_i}|" ></div>

#### count	行数

#### explained variance 解释方差
<div align=center><img src="http://latex.codecogs.com/gif.latex?explained Variance=\dfrac{SSR}{N}" ></div>

## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码

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

#### 脚本运行结果
```
Total Samples Number: 6.0
SSE: 15.0
SAE: 7.0
RMSE: 1.5811388300841898
R2: 0.8282442748091603
```


