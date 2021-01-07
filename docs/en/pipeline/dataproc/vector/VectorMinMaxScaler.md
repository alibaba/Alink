## Description
The transformer normalizes the value of the vector to [0,1] using the following formula:

 x_scaled = (x - eMin) / (eMax - eMin) * (maxV - minV) + minV;

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| min | Lower bound after transformation. | Double |  | 0.0 |
| max | Upper bound after transformation. | Double |  | 1.0 |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example

### Code

```python
data = np.array([["a", "10.0, 100"],\
    ["b", "-2.5, 9"],\
    ["c", "100.2, 1"],\
    ["d", "-99.9, 100"],\
    ["a", "1.4, 1"],\
    ["b", "-2.2, 9"],\
    ["c", "100.9, 1"]])
df = pd.DataFrame({"col" : data[:,0], "vec" : data[:,1]})
data = dataframeToOperator(df, schemaStr="col string, vec string",op_type="batch")
res = VectorMinMaxScaler()\
           .setSelectedCol("vec")
res.fit(data).transform(data).collectToDataframe()
```
### Results

col1|vec
----|---
a|0.5473107569721115,1.0
b|0.4850597609561753,0.08080808080808081
c|0.9965139442231076,0.0
d|0.0,1.0
a|0.5044820717131474,0.0
b|0.4865537848605578,0.08080808080808081
c|1.0,0.0
