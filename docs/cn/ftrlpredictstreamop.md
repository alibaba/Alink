# Ftrl 在线预测

## 算法介绍
实时更新ftrl 训练得到的模型流，并使用实时的模型对实时的数据进行预测。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 运行脚本
```python
 data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 2]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
streamData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='stream')

model = LogisticRegressionTrainBatchOp() \
			.setFeatureCols(["f0", "f1"]) \
			.setLabelCol("label") \
			.setMaxIter(5).linkFrom(batchData);


models = FtrlTrainStreamOp(model) \
			.setFeatureCols(["f0", "f1"]) \
			.setLabelCol("label") \
			.setTimeInterval(1) \
			.setAlpha(0.1) \
			.setBeta(0.1) \
			.setL1(0.1) \
			.setL2(0.1).setVectorSize(2).setWithIntercept(True) \
		    .linkFrom(streamData);

FtrlPredictStreamOp(model) \
        .setPredictionCol("pred") \
        .setReservedCols(["label"]) \
        .setPredictionDetailCol("details") \
        .linkFrom(models, streamData).print()
StreamOperator.execute()
```
#### 运行结果
```
label	pred	details
1	1	{"1":"0.9999917437501057","2":"8.2562498943117...
1	1	{"1":"0.965917838185468","2":"0.03408216181453...
2	2	{"1":"0.00658782416074899","2":"0.993412175839...
1	1	{"1":"0.9810760570397847","2":"0.0189239429602...
1	1	{"1":"0.9998904582473768","2":"1.0954175262323...
2	2	{"1":"0.00658782416074899","2":"0.993412175839...
1	1	{"1":"0.9999996598523875","2":"3.4014761252088...
2	2	{"1":"2.0589409516880153E-5","2":"0.9999794105...
```


```
