## Description
Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 Imputer Train will train a model for predict.
 Strategy support min, max, mean or value.
 If min, will replace missing value with min of the column.
 If max, will replace missing value with max of the column.
 If mean, will replace missing value with mean of the column.
 If value, will replace missing value with the value.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| strategy | the startegy to fill missing value, support mean, max, min or value | String |  | "mean" |
| fillValue | fill all missing values with fillValue | String |  | null |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |


## Script Example

```python
data = np.array([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1],
            [None, None, None]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

df = pd.DataFrame({"col1": data[:, 0], "col2": data[:, 1], "col3": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='batch')
         

# train
trainOp = ImputerTrainBatchOp()\
           .setSelectedCols(selectedColNames)

trainOp.linkFrom(inOp)

# batch predict
predictOp =  ImputerPredictBatchOp()
predictOp.linkFrom(trainOp, inOp).print()

# stream predict
sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')

predictStreamOp = MaxAbsScalerPredictStreamOp(trainOp)
predictStreamOp.linkFrom(sinOp).print()


StreamOperator.execute()
```

#### Results

```
 col1        col2  col3
0     a   10.000000   100
1     b   -2.500000     9
2     c  100.200000     1
3     d  -99.900000   100
4     a    1.400000     1
5     b   -2.200000     9
6     c  100.900000     1
7  None   15.414286    31
```








