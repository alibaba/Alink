## Description
StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| withMean | Centers the data with mean before scaling. | Boolean |  | true |
| withStd | Scales the data to unit standard deviation. true by default | Boolean |  | true |
| outputCols | Names of the output columns | String[] |  | null |


## Script Example

#### Script

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


sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')
                   

model = StandardScaler()\
           .setSelectedCols(selectedColNames)\
           .fit(inOp)

model.transform(inOp).print()

model.transform(sinOp).print()

StreamOperator.execute()

```

#### Result

```
  col1      col2      col3
0    a -0.078352  1.459581
1    b -0.259243 -0.481449
2    c  1.226961 -0.652089
3    d -1.668749  1.459581
4    a -0.202805 -0.652089
5    b -0.254902 -0.481449
6    c  1.237091 -0.652089
```

