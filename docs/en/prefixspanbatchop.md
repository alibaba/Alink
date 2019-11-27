## Description
PrefixSpan algorithm is used to mine frequent sequential patterns.
 The PrefixSpan algorithm is described in J. Pei, et al.,
 Mining Sequential Patterns by Pattern-Growth: The PrefixSpan Approach

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| itemsCol | Column name of transaction items | String | ✓ |  |
| minSupportCount | Minimum support count | Integer |  | -1 |
| minSupportPercent | Minimum support percent | Double |  | 0.02 |
| minConfidence | Minimum confidence | Double |  | 0.05 |
| maxPatternLength | Maximum frequent pattern length | Integer |  | 10 |


## Script Example
#### Code
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

#### Results

Output
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

Output

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


