

## 功能介绍
提供sql的union all语句功能


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |


## 脚本示例
#### 脚本代码

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
