## 功能介绍

RegexTokenizer支持对文本的切分和匹配操作。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| pattern | 分隔符/正则匹配符 | 如果gaps为True，pattern用于切分文档；如果gaps为False，会提取出匹配pattern的词。 | String |  | "\\s+" |
| gaps | 切分/匹配 | 如果gaps为True，pattern用于切分文档；如果gaps为False，会提取出匹配pattern的词。 | Boolean |  | true |
| minTokenLength | 词语最短长度 | 词语的最短长度，小于这个值的词语会被过滤掉 | Integer |  | 1 |
| toLowerCase | 是否转换为小写 | 转换为小写 | Boolean |  | true |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



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
op = RegexTokenizerBatchOp().setSelectedCol("text").setGaps(False).setToLowerCase(True).setOutputCol("token").setPattern("\\w+")

op.linkFrom(inOp1).print()

inOp2 = dataframeToOperator(df, schemaStr='id long, text string', op_type='stream')
op = RegexTokenizerStreamOp().setSelectedCol("text").setGaps(False).setToLowerCase(True).setOutputCol("token").setPattern("\\w+")
op.linkFrom(inOp2).print()

StreamOperator.execute()
```

#### 脚本运行结果

```
id	text	token
0	0	That is an English Book!	that is an english book
1	2	Have a good day!	have a good day
2	1	Do you like math?	do you like math

```
