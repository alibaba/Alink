# 朴素贝叶斯流预测
## 功能介绍

* 朴素贝叶斯是一个多分类算法
* 朴素贝叶斯组件支持稀疏、稠密两种数据格式
* 朴素贝叶斯组件支持带样本权重的训练

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```
data = np.array([
    [1.0, 1.0, 0.0, 1.0, 1],
    [1.0, 0.0, 1.0, 1.0, 1],
    [1.0, 0.0, 1.0, 1.0, 1],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [1.0, 1.0, 1.0, 1.0, 1],
    [0.0, 1.0, 1.0, 0.0, 0]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "f2": data[:, 2],
                   "f3": data[:, 3],
                   "label": data[:, 4]})
df["label"] = df["label"].astype('int')
batchData = dataframeToOperator(df, schemaStr='f0 double, f1 double, f2 double, f3 double, label int', op_type='batch')
streamData = dataframeToOperator(df, schemaStr='f0 double, f1 double, f2 double, f3 double, label int', op_type='stream')

# load data
colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)

predictor = NaiveBayesPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(streamData).print()
```
#### 运行结果

f0 | f1 | f2 | f3 | label | pred
---|----|----|----|-------|----
1.0|1.0|0.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
1.0|1.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0





