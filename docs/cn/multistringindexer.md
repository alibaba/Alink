
## 功能介绍
MultiStringIndexer训练组件的作用是训练一个模型用于将多列字符串映射为整数。

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "keep" |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| stringOrderType | Token排序方法 | Token排序方法 | String |  | "random" |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


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

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='batch')

stringindexer = MultiStringIndexer() \
    .setSelectedCols(["f0"]) \
    .setOutputCols(["f0_indexed"]) \
    .setStringOrderType("frequency_asc")

stringindexer.fit(data).transform(data).print()
```

#### 脚本运行结果

模型表：
```
           f0  f0_indexed
0    football           2
1    football           2
2    football           2
3  basketball           1
4  basketball           1
5      tennis           0
```
