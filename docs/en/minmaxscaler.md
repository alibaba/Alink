## Description
MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 to a specific range [min, max). (often [0, 1]).

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| min | Lower bound after transformation. | Double |  | 0.0 |
| max | Upper bound after transformation. | Double |  | 1.0 |
| outputCols | Names of the output columns | String[] |  | null |


## Script Example

#### Script


```
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
                   

model = MinMaxScaler()\
           .setSelectedCols(selectedColNames)\
           .fit(inOp)

model.transform(inOp).print()

model.transform(sinOp).print()

StreamOperator.execute()
```

#### Results

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
