## Description
StratifiedSample with given ratio with/without replacement.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| strataCol | strata col name. | String | ✓ |  |
| strataRatio | strata ratio. | Double |  | -1.0 |
| strataRatios | strata ratios. a:0.1,b:0.3 | String | ✓ |  |
| withReplacement | Indicates whether to enable sampling with replacement, default is without replacement | Boolean |  | false |

## Script Example

### Code

```python
data = np.array([
        ['a',0.0,0.0],
        ['a',0.2,0.1],
        ['b',0.2,0.8],
        ['b',9.5,9.7],
        ['b',9.1,9.6],
        ['b',9.3,9.9]
    ])

df_data = pd.DataFrame({
    "x1": data[:, 0],
    "x2": data[:, 1],
    "x3": data[:, 2]
})


# batch 
batchData = dataframeToOperator(df_data, schemaStr='x1 string, x2 double, x3 double', op_type='batch')
sampleOp = StratifiedSampleBatchOp()\
       .setStrataCol("x1")\
       .setStrataRatios("a:0.5,b:0.5")

batchData.link(sampleOp).print()

```

### Results

```
  x1   x2   x3
0  a  0.0  0.0
1  b  0.2  0.8
2  b  9.5  9.7
3  b  9.3  9.9
```
