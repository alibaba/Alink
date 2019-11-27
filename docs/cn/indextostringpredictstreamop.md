## 功能介绍
基于StringIndexer模型，将一列整数映射为字符串。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| modelName | 模型名字 | 模型名字 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
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

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="stream")

stringIndexer = StringIndexer() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc")

indexed = stringIndexer.fit(data).transform(data)

indexToString = IndexToString() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0_indexed") \
    .setOutputCol("f0_indxed_unindexed")

indexToString.transform(indexed).print()
```

#### 脚本运行结果

```
f0|f0_indexed|f0_indxed_unindexed
--|----------|-------------------
football|2|football
football|2|football
football|2|football
basketball|1|basketball
basketball|1|basketball
tennis|0|tennis
```
