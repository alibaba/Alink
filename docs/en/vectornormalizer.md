## Description
Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It
 takes parameter p, which specifies the p-norm used for normalization. This normalization can help standardize your
 input data and improve the behavior of learning algorithms.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| p | number of degree. | Double |  | 2.0 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example

#### Script
``` python
data = np.array([["1:3,2:4,4:7", 1],\
    ["0:3,5:5", 3],\
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="batch")
VectorNormalizer().setSelectedCol("vec").setOutputCol("vec_norm").transform(data).collectToDataframe()
```
#### Result


| vec         | id   | vec_norm                                 |
| ----------- | ---- | ---------------------------------------- |
| 1:3,2:4,4:7 | 1    | 1:0.34874291623145787 2:0.46499055497527714 4:0.813733471206735 |
| 0:3,5:5     | 3    | 0:0.5144957554275265 5:0.8574929257125441 |
| 2:4,4:5     | 4    | 2:0.6246950475544243 4:0.7808688094430304 |


