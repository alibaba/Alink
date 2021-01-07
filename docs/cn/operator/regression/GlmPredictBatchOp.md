# GLM预测

## 功能介绍
使用GLM模型，对数据进行预测

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| linkPredResultCol | 连接函数结果的列名 | 连接函数结果的列名 | String |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 脚本示例
#### 脚本代码
```python

# data
data = np.array([
    [1.6094,118.0000,69.0000,1.0000,2.0000],
    [2.3026,58.0000,35.0000,1.0000,2.0000],
    [2.7081,42.0000,26.0000,1.0000,2.0000],
    [2.9957,35.0000,21.0000,1.0000,2.0000],
    [3.4012,27.0000,18.0000,1.0000,2.0000],
    [3.6889,25.0000,16.0000,1.0000,2.0000],
    [4.0943,21.0000,13.0000,1.0000,2.0000],
    [4.3820,19.0000,12.0000,1.0000,2.0000],
    [4.6052,18.0000,12.0000,1.0000,2.0000]
])


df = pd.DataFrame({"u": data[:, 0], "lot1": data[:, 1], "lot2": data[:, 2], "offset": data[:, 3], "weights": data[:, 4]})
source = dataframeToOperator(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double', op_type='batch')

featureColNames = ["lot1", "lot2"]
labelColName = "u"

# train
train = GlmTrainBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

source.link(train)

# predict
predict =  GlmPredictBatchOp()\
                .setPredictionCol("pred")

predict.linkFrom(train, source)


# eval
eval =  GlmEvaluationBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

eval.linkFrom(train, source)

predict.print()
eval.print()

```

#### 脚本运行结果

 u |  lot1|  lot2 | offset | weights  |    pred
----|----|------|-----------|------|-----------    
0 | 1.6094 | 118.0 | 69.0  |   1.0    |  2.0 | 0.378525
1 | 2.3026 |  58.0 | 35.0  |   1.0  |    2.0 | 0.970639
2|  2.7081  | 42.0 | 26.0 |    1.0  |    2.0 | 1.126458
3 | 2.9957 |  35.0 | 21.0 |    1.0  |    2.0 | 1.227753
4 | 3.4012 |  27.0 | 18.0 |    1.0  |    2.0 | 1.258898
5 | 3.6889 |  25.0 | 16.0 |    1.0  |    2.0 | 1.305654
6 | 4.0943 |  21.0 | 13.0 |    1.0 |     2.0|  1.367991
7 | 4.3820 |  19.0 | 12.0 |    1.0 |     2.0 | 1.383571
8 | 4.6052 |  18.0 | 12.0 |    1.0  |    2.0 | 1.375774




