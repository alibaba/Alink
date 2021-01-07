## 功能介绍
数据结构转换，将多列数据（可以是向量列也可以是数值列）转化为一列向量数据。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR" |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例
### 脚本代码
```python
data = np.array([["0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0"],\
["1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0"],\
["2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0"]])
df = pd.DataFrame({"id" : data[:,0], "c0" : data[:,1], "c1" : data[:,2]})
data = dataframeToOperator(df, schemaStr="id string, c0 string, c1 string",op_type="batch")

res = VectorAssembler()\
			.setSelectedCols(["c0", "c1"])\
			.setOutputCol("table2vec")
res.transform(data).collectToDataframe()
```

### 脚本运行结果

<img src="https://img.alicdn.com/tfs/TB1_YGWokT2gK0jSZPcXXcKkpXa-448-114.jpg">
