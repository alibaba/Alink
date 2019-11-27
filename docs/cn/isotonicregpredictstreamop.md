## 功能介绍

保序回归在观念上是寻找一组非递减的片段连续线性函数（piecewise linear continuous functions），即保序函数，使其与样本尽可能的接近。
## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->
## 脚本示例
#### 脚本代码
```python
data = np.array([[0.35, 1],\
[0.6, 1],\
[0.55, 1],\
[0.5, 1],\
[0.18, 0],\
[0.1, 1],\
[0.8, 1],\
[0.45, 0],\
[0.4, 1],\
[0.7, 0],\
[0.02, 1],\
[0.3, 0],\
[0.27, 1],\
[0.2, 0],\
[0.9, 1]])

df = pd.DataFrame({"feature" : data[:,0], "label" : data[:,1]})
data = dataframeToOperator(df, schemaStr="label double, feature double",op_type="batch")
dataStream = dataframeToOperator(df, schemaStr="label double, feature double",op_type="stream")
trainOp = IsotonicRegTrainBatchOp()\
            .setFeatureCol("feature")\
			.setLabelCol("label")
model = trainOp.linkFrom(data)
predictOp = IsotonicRegPredictStreamOp(model).setPredictionCol("result")
res = predictOp.linkFrom(dataStream)
res.print()
```

#### 脚本运行结果
##### 模型结果
| model_id   | model_info |
| --- | --- |
| 0          | {"vectorCol":"\"col2\"","featureIndex":"0","featureCol":null} |
| 1048576    | [0.02,0.3,0.35,0.45,0.5,0.7] |
| 2097152    | [0.5,0.5,0.6666666865348816,0.6666666865348816,0.75,0.75] |
##### 预测结果
| col1       | col2       | col3       | pred       |
| --- | --- | --- | --- |
| 1.0        | 0.9        | 1.0        | 0.75       |
| 0.0        | 0.7        | 1.0        | 0.75       |
| 1.0        | 0.35       | 1.0        | 0.6666666865348816 |
| 1.0        | 0.02       | 1.0        | 0.5        |
| 1.0        | 0.27       | 1.0        | 0.5        |
| 1.0        | 0.5        | 1.0        | 0.75       |
| 0.0        | 0.18       | 1.0        | 0.5        |
| 0.0        | 0.45       | 1.0        | 0.6666666865348816 |
| 1.0        | 0.8        | 1.0        | 0.75       |
| 1.0        | 0.6        | 1.0        | 0.75       |
| 1.0        | 0.4        | 1.0        | 0.6666666865348816 |
| 0.0        | 0.3        | 1.0        | 0.5        |
| 1.0        | 0.55       | 1.0        | 0.75       |
| 0.0        | 0.2        | 1.0        | 0.5        |
| 1.0        | 0.1        | 1.0        | 0.5        |
