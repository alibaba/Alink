# Ridge回归算法

## 功能介绍
* Ridge回归是一个回归算法
* Ridge回归组件支持稀疏、稠密两种数据格式
* Ridge回归组件支持带样本权重的训练


## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```python
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
streamData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='stream')
colnames = ["f0","f1"]
ridge = RidgeRegTrainBatchOp().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ridge)

predictor = LinearRegPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(streamData).print()
StreamOperator.execute()
```
#### 运行结果
f0 | f1 | f2 | label | pred
---|----|----|-------|-----
1.0|7.0|9.0|16.8|16.614452974656647
1.0|3.0|3.0|6.7|6.754928617036061
1.0|2.0|4.0|6.9|6.871072594920224
1.0|3.0|4.0|8.0|7.787338643951784




