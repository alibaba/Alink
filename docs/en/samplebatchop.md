## Description
Sample the input data with given ratio with or without replacement.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| ratio | sampling ratio, it should be in range of [0, 1] | Double | ✓ |  |
| withReplacement | Indicates whether to enable sampling with replacement, default is without replcement | Boolean |  | false |

## Script Example

### Code

```python
data = data = np.array([
       ["0,0,0"],
       ["0.1,0.1,0.1"],
       ["0.2,0.2,0.2"],
       ["9,9,9"],
       ["9.1,9.1,9.1"],
       ["9.2,9.2,9.2"]
])
    
df = pd.DataFrame({"Y": data[:, 0]})

# batch source 
inOp = dataframeToOperator(df, schemaStr='Y string', op_type='batch')

sampleOp = SampleBatchOp()\
        .setRatio(0.3)\
        .setWithReplacement(False)

inOp.link(sampleOp).print()
```
### Results

|Y|
|---|
|0,0,0|
|0.2,0.2,0.2|




