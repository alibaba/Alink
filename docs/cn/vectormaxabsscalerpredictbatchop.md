## 功能介绍

 vector绝对值最大标准化是对vector数据按照最大值和最小值进行标准化的组件, 将数据归一到-1和1之间。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |<!-- This is the end of auto-generated parameter info -->



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
trainOp = VectorMaxAbsScalerTrainBatchOp()\
           .setSelectedCol("vec")
model = trainOp.linkFrom(data) 

batchPredictOp = VectorMaxAbsScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).collectToDataframe()
```
#### 结果

col1|vec
----|---
c|1.0,0.01
b|-0.024777006937561942,0.09
d|-0.9900891972249752,1.0
a|0.09910802775024777,1.0
b|-0.02180376610505451,0.09
c|0.9930624380574826,0.01
a|0.013875123885034686,0.01



