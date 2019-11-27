## 功能介绍

 将向量转为表，向量的每一维数据都转为表的列。
 
## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->
## 脚本示例

#### 脚本

```python
data = np.array([["a", "10.0, 100"],\
    ["b", "-2.5, 9"],\
    ["c", "100.2, 1"],\
    ["d", "-99.9, 100"],\
    ["a", "1.4, 1"],\
    ["b", "-2.2, 9"],\
    ["c", "100.9, 1"]])
df = pd.DataFrame({"col" : data[:,0], "vec" : data[:,1]})
data = dataframeToOperator(df, schemaStr="col string, vec string",op_type="batch")
VectorToColumnsBatchOp().setSelectedCol("vec").setOutputCols(["f0", "f1"]).linkFrom(data).collectToDataframe()
```
#### 结果

<img src="https://img.alicdn.com/tfs/TB1IMm7oXP7gK0jSZFjXXc5aXXa-232-226.jpg">