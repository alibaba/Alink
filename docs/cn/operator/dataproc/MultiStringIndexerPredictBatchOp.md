
# MultiStringIndexer预测

## 功能介绍
基于MultiStringIndexer模型，将多列字符串映射为整数。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |


## 脚本示例
### 脚本代码
```python
data = np.array([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

df_data = pd.DataFrame({
    "f0": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="batch")

stringindexer = MultiStringIndexerTrainBatchOp() \
    .setSelectedCols(["f0"]) \
    .setStringOrderType("frequency_asc")

predictor = MultiStringIndexerPredictBatchOp().setSelectedCols(["f0"]).setOutputCols(["f0_indexed"])

model = stringindexer.linkFrom(data)
predictor.linkFrom(model, data).print()
```

### 脚本运行结果

```
           f0  f0_indexed
0    football           2
1    football           2
2    football           2
3  basketball           1
4  basketball           1
5      tennis           0
```
