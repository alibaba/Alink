## 功能介绍
多分类评估是对多分类算法的预测结果进行效果评估。

流式的实验支持累计统计和窗口统计。

给出整体的评估指标包括Precision、Recall、F-Measure、Sensitivity、Accuracy、Specificity和Kappa。

#### Precision
<div align=center><img src="http://latex.codecogs.com/gif.latex?Precision = \dfrac{TP}{TP + FP}" ></div>

#### Recall
<div align=center><img src="http://latex.codecogs.com/gif.latex?Recall = \dfrac{TP}{TP + FN}" ></div>

#### F-Measure
<div align=center><img src="http://latex.codecogs.com/gif.latex?F1=\dfrac{2TP}{2TP+FP+FN}=\dfrac{2\cdot Precision \cdot Recall}{Precision+Recall}" ></div>

#### Sensitivity
<div align=center><img src="http://latex.codecogs.com/gif.latex?Sensitivity=\dfrac{TP}{TP+FN}" ></div>

#### Accuracy
<div align=center><img src="http://latex.codecogs.com/gif.latex?Accuray=\dfrac{TP + TN}{TP + TN + FP + FN}" ></div>

#### Specificity
<div align=center><img src="http://latex.codecogs.com/gif.latex?Specificity=\dfrac{TN}{FP+T}" ></div>

#### Kappa
<div align=center><img src="http://latex.codecogs.com/gif.latex?p_a =\dfrac{TP + TN}{TP + TN + FP + FN}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?p_e = \dfrac{(TN + FP) * (TN + FN) + (FN + TP) * (FP + TP)}{(TP + TN + FP + FN) * (TP + TN + FP + FN)}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?kappa = \dfrac{p_a - p_e}{1 - p_e}" ></div>

#### Logloss
<div align=center><img src="http://latex.codecogs.com/gif.latex?logloss=- \dfrac{1}{N}\sum_{i=1}^N \sum_{j=1}^My_{i,j}log(p_{i,j})" ></div>

## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| timeInterval | 时间间隔 | 流式数据统计的时间间隔 | Integer |  | 3 |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd
data = np.array([
    ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
	["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
	["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
	["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
	["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]])
	
df = pd.DataFrame({"label": data[:, 0], "detailInput": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')

metrics = EvalMultiClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp).collectMetrics()
print("Prefix0 accuracy:", metrics.getAccuracy("prefix0"))
print("Prefix1 recall:", metrics.getRecall("prefix1"))
print("Macro Precision:", metrics.getMacroPrecision())
print("Micro Recall:", metrics.getMicroRecall())
print("Weighted Sensitivity:", metrics.getWeightedSensitivity())


inOp = StreamOperator.fromDataframe(df, schemaStr='label string, detailInput string')
EvalMultiClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(1).linkFrom(inOp).print()
StreamOperator.execute()
```



