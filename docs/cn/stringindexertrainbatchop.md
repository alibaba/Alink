# StringIndexer训练

## 功能介绍
StringIndexer训练组件的作用是训练一个模型用于将单列字符串映射为整数。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| modelName | 模型名字 | 模型名字 | String |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| stringOrderType | Token排序方法 | Token排序方法 | String |  | "random" |<!-- This is the end of auto-generated parameter info -->


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

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="batch")

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)
model.print()
```

#### 脚本运行结果

模型表：
```
        token  token_index
0      tennis            0
1  basketball            1
2    football            2
```


