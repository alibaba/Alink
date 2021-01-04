## Description
Generalized Linear Model stream predict. https://en.wikipedia.org/wiki/Generalized_linear_model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| linkPredResultCol | link predict col name of output | String |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
#### Code
```python

# data
data = np.array([
    [1.6094,118.0000,69.0000,1.0000,2.0000],
    [2.3026,58.0000,35.0000,1.0000,2.0000],
    [2.7081,42.0000,26.0000,1.0000,2.0000],
    [2.9957,35.0000,21.0000,1.0000,2.0000],
    [3.4012,27.0000,18.0000,1.0000,2.0000],
    [3.6889,25.0000,16.0000,1.0000,2.0000],
    [4.0943,21.0000,13.0000,1.0000,2.0000],
    [4.3820,19.0000,12.0000,1.0000,2.0000],
    [4.6052,18.0000,12.0000,1.0000,2.0000]
])


df = pd.DataFrame({"u": data[:, 0], "lot1": data[:, 1], "lot2": data[:, 2], "offset": data[:, 3], "weights": data[:, 4]})
source = dataframeToOperator(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double', op_type='batch')


featureColNames = ["lot1", "lot2"]
labelColName = "u"

# train
train = GlmTrainBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

source.link(train)

#  batch predict
predict =  GlmPredictBatchOp()\
                .setPredictionCol("pred")

predict.linkFrom(train, source)
predict.print()

# eval
eval =  GlmEvaluationBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

eval.linkFrom(train, source)
eval.print()


# stream predict
source_stream = dataframeToOperator(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double', op_type='stream')

predict_stream =  GlmPredictStreamOp(train)\
                .setPredictionCol("pred")

predict_stream.linkFrom(source_stream)
predict_stream.print()

```

#### Results

 u |  lot1|  lot2 | offset | weights  |    pred
----|----|------|-----------|------|-----------    
0 | 1.6094 | 118.0 | 69.0  |   1.0    |  2.0 | 0.378525
1 | 2.3026 |  58.0 | 35.0  |   1.0  |    2.0 | 0.970639
2|  2.7081  | 42.0 | 26.0 |    1.0  |    2.0 | 1.126458
3 | 2.9957 |  35.0 | 21.0 |    1.0  |    2.0 | 1.227753
4 | 3.4012 |  27.0 | 18.0 |    1.0  |    2.0 | 1.258898
5 | 3.6889 |  25.0 | 16.0 |    1.0  |    2.0 | 1.305654
6 | 4.0943 |  21.0 | 13.0 |    1.0 |     2.0|  1.367991
7 | 4.3820 |  19.0 | 12.0 |    1.0 |     2.0 | 1.383571
8 | 4.6052 |  18.0 | 12.0 |    1.0  |    2.0 | 1.375774







