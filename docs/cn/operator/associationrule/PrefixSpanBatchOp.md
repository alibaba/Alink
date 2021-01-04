# PrefixSpan

## 功能介绍
PrefixSpan是从输入序列中选取所有满足支持度的频繁子序列。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemsCol | 项集列名 | 项集列名 | String | ✓ |  |
| minSupportCount | 最小支持度数目 | 最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用 | Integer |  | -1 |
| minSupportPercent | 最小支持度占比 | 最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用 | Double |  | 0.02 |
| minConfidence | 最小置信度 | 最小置信度 | Double |  | 0.05 |
| maxPatternLength | 最大频繁项集长度 | 最大频繁项集长度 | Integer |  | 10 |



## 脚本示例
### 脚本代码
```python
data = np.array([
    ["a;a,b,c;a,c;d;c,f"],
    ["a,d;c;b,c;a,e"],
    ["e,f;a,b;d,f;c;b"],
    ["e;g;a,f;c;b;c"],
])

df_data = pd.DataFrame({
    "sequence": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='sequence string', op_type='batch')

prefixSpan = PrefixSpanBatchOp() \
    .setItemsCol("sequence") \
    .setMinSupportCount(3)

prefixSpan.linkFrom(data)

prefixSpan.print()
prefixSpan.getSideOutput(0).print()
```

输入说明：一个sequence由多个element组成，element之间用分号分隔；一个element由多个item组成，item间用逗号分隔。

### 脚本运行结果

频繁项集输出：
```
    itemset  supportcount  itemcount
0        a             4          1
1      a;c             4          2
2    a;c;c             3          3
3    a;c;b             3          3
4      a;b             4          2
5        b             4          1
6      b;c             3          2
7        c             4          1
8      c;c             3          2
9      c;b             3          2
10       d             3          1
11     d;c             3          2
12       e             3          1
13       f             3          1
```

关联规则输出：

```
     rule  chain_length  support  confidence  transaction_count
0    a=>c             2     1.00        1.00                  4
1  a;c=>c             3     0.75        0.75                  3
2  a;c=>b             3     0.75        0.75                  3
3    a=>b             2     1.00        1.00                  4
4    b=>c             2     0.75        0.75                  3
5    c=>c             2     0.75        0.75                  3
6    c=>b             2     0.75        0.75                  3
7    d=>c             2     0.75        1.00                  3
```

