## 功能介绍

Tokenizer(标记器)是将文本（如句子）分解成单个词语（通常是单词）的过程。

## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
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

op = TokenizerBatchOp().setSelectedCol("text")
print(BatchOperator.collectToDataframe(op.linkFrom(inOp1)))

inOp2 = dataframeToOperator(df, schemaStr='id long, text string', op_type='stream')
op = TokenizerStreamOp().setSelectedCol("text")
op.linkFrom(inOp2).print()

StreamOperator.execute()
```

#### 脚本运行结果
```
	id	text
0	1	do you like math?
1	0	that is an english book!
2	2	have a good day!

```

