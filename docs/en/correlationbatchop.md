## Description
Calculating the correlation between two series of data is a common operation in Statistics.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] |  | null |
| method | method: pearson, spearman. default pearson | String |  | "pearson" |


## Script Example

#### Script

```python
data = np.array([
         [0.0,0.0,0.0],
         [0.1,0.2,0.1],
         [0.2,0.2,0.8],
         [9.0,9.5,9.7],
         [9.1,9.1,9.6],
         [9.2,9.3,9.9]])

df = pd.DataFrame({"x1": data[:, 0], "x2": data[:, 1], "x3": data[:, 2]})
source = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='batch')


corr = CorrelationBatchOp()\
            .setSelectedCols(["x1","x2","x3"])

correlation = source.link(corr).collectCorrelationResult()
print(correlation.getCorrelation())

```
#### Result

```
[[1.0, 0.9994126290576466, 0.9990251498141454], [0.9994126290576466, 1.0, 0.9985989260453877], [0.9990251498141454, 0.9985989260453877, 1.0]]
```



