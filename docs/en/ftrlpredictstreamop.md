## Description
Ftrl predictor receive two stream : model stream and data stream. It using updated model by model stream real-time,
 and using the newest model predict data stream.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |


## Script Example
#### Script
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
#### Result
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

