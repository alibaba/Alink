## Description
Apply the "group by" operation on the input batch operator.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| groupByPredicate | Group by clause. | String | ✓ |  |
| selectClause | Select clause | String | ✓ |  |


## Script Example
#### Code

```python
data = data.link(GroupByBatchOp().setGroupByPredicate("f1").setSelectClause("f1,avg(f2) as f2"))
```

