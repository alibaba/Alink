## Description
Union with another <code>BatchOperator</code>. The duplicated records are kept.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |


## Script Example
#### Code

```python
data = {
  'f1': ['changjiang', 'huanghe', 'zhujiang', 'changjiang', 'huanghe', 'zhujiang'],
  'f2': [2000, 2001, 2002, 2001, 2002, 2003],
  'f3': [1.5, 1.7, 3.6, 2.4, 2.9, 3.2]
}
df_data = pd.DataFrame(data)
schema = 'f1 string, f2 bigint, f3 double'
data1 = BatchOperator.fromDataframe(df_data, schemaStr=schema)
data2 = BatchOperator.fromDataframe(df_data, schemaStr=schema)

unionAllOp = UnionAllBatchOp()
output = unionAllOp.linkFrom(data1, data2)
```
