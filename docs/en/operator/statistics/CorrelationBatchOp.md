## Description
Calculating the correlation between two series of data is a common operation in Statistics.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] |  | null |
| method | method: PEARSON, SPEARMAN. default PEARSON | String |  | "PEARSON" |

## Script Example

### Code

```python
import numpy as np
import pandas as pd

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

corr = source.link(corr).collectCorrelation()
print(corr)

```
### Results

```
colName|x1|x2|x3
-------|--|--|--
x1|1.0000|0.9994|0.9990
x2|0.9994|1.0000|0.9986
x3|0.9990|0.9986|1.0000
```


