## 功能介绍

主成分分析，是考察多个变量间相关性一种多元统计方法，研究如何通过少数几个主成分来揭示多个变量间的内部结构，即从原始变量中导出少数几个主成分，使它们尽可能多地保留原始变量的信息，且彼此间互不相关，作为新的综合指标。详细介绍请见维基百科链接[wiki](https://en.wikipedia.org/wiki/Principal_component_analysis)。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| k | 降维后的维度 | 降维后的维度 | Integer | ✓ |  |
| calculationType | 计算类型 | 计算类型，包含"CORR", "COV"两种。 | String |  | "CORR" |



## 脚本示例

### 脚本代码

```python
data = np.array([
        [0.0,0.0,0.0],
        [0.1,0.2,0.1],
        [0.2,0.2,0.8],
        [9.0,9.5,9.7],
        [9.1,9.1,9.6],
        [9.2,9.3,9.9]
])

df = pd.DataFrame({"x1": data[:, 0], "x2": data[:, 1], "x3": data[:, 2]})

# batch source 
inOp = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='batch')

trainOp = PcaTrainBatchOp()\
       .setK(2)\
       .setSelectedCols(["x1","x2","x3"])

predictOp = PcaPredictBatchOp()\
        .setPredictionCol("pred")

# batch train
inOp.link(trainOp)

# batch predict
predictOp.linkFrom(trainOp,inOp)

predictOp.print()

# stream predict
inStreamOp = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='stream')

predictStreamOp = PcaPredictStreamOp(trainOp)\
        .setPredictionCol("pred")

predictStreamOp.linkFrom(inStreamOp)

predictStreamOp.print()

StreamOperator.execute()
```
### 脚本运行结果

x1|x2|x3|pred
---|---|---|----
9.0|9.5|9.7|3.2280384305400736,1.1516225426477789E-4
0.2|0.2|0.8|0.13565076707329407,0.09003329494282108
9.2|9.3|9.9|3.250783163664603,0.0456526246528135
9.1|9.1|9.6|3.182618319978973,0.027469531992220464
0.1|0.2|0.1|0.045855205015063565,-0.012182917696915518
0.0|0.0|0.0|0.0,0.0



