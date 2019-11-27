## 功能介绍

 vector归一化是对vector数据进行归一的组件, 将数据归一到min和max之间。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| min | 归一化的下界 | 归一化的下界 | Double |  | 0.0 |
| max | 归一化的上界 | 归一化的上界 | Double |  | 1.0 |<!-- This is the end of auto-generated parameter info -->

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
trainOp = VectorMinMaxScalerTrainBatchOp()\
           .setSelectedCol("vec")
model = trainOp.linkFrom(data) 

batchPredictOp = VectorMinMaxScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).collectToDataframe()
```
#### 结果

col1|vec
----|---
a|0.5473107569721115,1.0
b|0.4850597609561753,0.08080808080808081
c|0.9965139442231076,0.0
d|0.0,1.0
a|0.5044820717131474,0.0
b|0.4865537848605578,0.08080808080808081
c|1.0,0.0



