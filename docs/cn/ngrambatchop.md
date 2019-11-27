## 功能介绍

本组件对于每行文本生成它的NGram存储。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| n | nGram长度 | nGram长度 | Integer |  | 2 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```python
data = np.array([
    [0, 'That is an English Book!'],
    [1, 'Do you like math?'],
    [2, 'Have a good day!']
])

df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp1 = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')

op = NGramBatchOp().setSelectedCol("text")
print(BatchOperator.collectToDataframe(op.linkFrom(inOp1)))

inOp2 = dataframeToOperator(df, schemaStr='id long, text string', op_type='stream')
op = NGramStreamOp().setSelectedCol("text")
op.linkFrom(inOp2).print()

StreamOperator.execute()
```

#### 脚本运行结果
```
	id	text
0	2	Have_a a_good good_day!
1	1	Do_you you_like like_math?
2	0	That_is is_an an_English English_Book!

```
