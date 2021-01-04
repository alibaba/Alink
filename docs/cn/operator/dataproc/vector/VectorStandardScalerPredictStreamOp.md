# Vector标准化流算法

## 功能介绍

 标准化是对数据进行按正态化处理的组件
 
## 参数说明 

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 脚本示例

### 脚本代码

```python
data = np.array([\
["a", "10.0, 100"],\
["b", "-2.5, 9"],\
["c", "100.2, 1"],\
["d", "-99.9, 100"],\
["a", "1.4, 1"],\
["b", "-2.2, 9"],\
["c", "100.9, 1"]])
df = pd.DataFrame({"col1":data[:,0], "vec":data[:,1]})
data = dataframeToOperator(df, schemaStr="col1 string, vec string",op_type="batch")
colnames = ["col1", "vec"]
selectedColName = "vec"

trainOp = VectorStandardScalerTrainBatchOp()\
           .setSelectedCol(selectedColName)
       
model = trainOp.linkFrom(data)

#batch predict                  
batchPredictOp = VectorStandardScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).print()

#stream predict
streamData = dataframeToOperator(df, schemaStr="col1 string, vec string",op_type="stream")

streamPredictOp = VectorStandardScalerPredictStreamOp(trainOp)
streamData.link(streamPredictOp).print()

StreamOperator.execute()
```
### 脚本运行结果

col1|vec
----|---
a|-0.07835182408093559,1.4595814453461897
c|1.2269606224811418,-0.6520885789229323
b|-0.2549018445693762,-0.4814485769617911
a|-0.20280511721213143,-0.6520885789229323
c|1.237090541689495,-0.6520885789229323
b|-0.25924323851581327,-0.4814485769617911
d|-1.6687491397923802,1.4595814453461897



