# SOS

## 功能介绍
SOS (Stochastic Outlier Selection）是一种affinity based离群点检测算法。
它通常用于过滤掉噪音样本，从而使得机器学习的模型更准确。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| perplexity | 邻近因子 | 邻近因子。它的近似含义是当某个点的近邻个数小于"邻近因子"个时，这个点的离群score会比较高。 | Double |  | 4.0 |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
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

#### 脚本运行结果

features|outlier_score
--------|-------------
1.0,1.0|0.12396819612216292
0.0,0.0|0.27815186043725715
0.0,1.0|0.24136320497783578
1.0,0.0|0.24136320497783578
5.0,5.0|0.9998106220648153

