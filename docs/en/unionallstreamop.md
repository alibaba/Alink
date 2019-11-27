## Description
Union two stream operators. The duplicated records are kept.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |



## Script Example
#### Code

```python
unionAllOp = UnionAllStreamOp()
output = unionAllOp.linkFrom(data1, data2)
```

