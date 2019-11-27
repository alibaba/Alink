## Description
Ftrl algorithm receive train data streams, using the training samples to update model element by element, and output
 model after every time interval.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| vectorSize | vector size of embedding | Integer | ✓ |  |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| withIntercept | Whether has intercept or not, default is true | Boolean |  | true |
| timeInterval | time interval | Integer |  | 1800 |
| l1 | the L1-regularized parameter. | Double |  | 0.0 |
| l2 | the L2-regularized parameter. | Double |  | 0.0 |
| alpha | alpha | Double |  | 0.1 |
| beta | beta | Double |  | 1.0 |


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


