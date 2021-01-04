## Description
MaxAbsScaler transforms a dataSet of rows, rescaling each feature to range
 [-1, 1] by dividing through the maximum absolute value in each feature.
 MaxAbsScalerTrain will train a model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |

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
trainOp = MaxAbsScalerTrainBatchOp()\
           .setSelectedCols(selectedColNames)

trainOp.linkFrom(inOp)

# batch predict
predictOp = MaxAbsScalerPredictBatchOp()
predictOp.linkFrom(trainOp, inOp).print()

# stream predict
sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')

predictStreamOp = MaxAbsScalerPredictStreamOp(trainOp)
predictStreamOp.linkFrom(sinOp).print()


StreamOperator.execute()

```

### Results

```
  col1      col2  col3
0    a  0.099108  1.00
1    b -0.024777  0.09
2    c  0.993062  0.01
3    d -0.990089  1.00
4    a  0.013875  0.01
5    b -0.021804  0.09
6    c  1.000000  0.01

```




