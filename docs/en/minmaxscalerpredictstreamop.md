## Description
MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 to a specific range [min, max). (often [0, 1]).
 MinMaxScalerPredict will scale the dataset with model which trained from MaxAbsTrain.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| numThreads | Thread number of operator. | Integer |  | 1 |
| outputCols | Names of the output columns | String[] |  | null |

## Script Example

### Code

```python
data = np.array([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

df = pd.DataFrame({"col1": data[:, 0], "col2": data[:, 1], "col3": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='batch')
         

# train
trainOp = MinMaxScalerTrainBatchOp()\
           .setSelectedCols(selectedColNames)

trainOp.linkFrom(inOp)

# batch predict
predictOp = MinMaxScalerPredictBatchOp()
predictOp.linkFrom(trainOp, inOp).print()

# stream predict
sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')

predictStreamOp = MinMaxScalerPredictStreamOp(trainOp)
predictStreamOp.linkFrom(sinOp).print()


StreamOperator.execute()
```

### Results

```
  col1      col2      col3
0    a  0.547311  1.000000
1    b  0.485060  0.080808
2    c  0.996514  0.000000
3    d  0.000000  1.000000
4    a  0.504482  0.000000
5    b  0.486554  0.080808
6    c  1.000000  0.000000
```



