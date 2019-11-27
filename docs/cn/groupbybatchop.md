
## 功能介绍
提供sql的group by语句功能

## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| groupByPredicate | groupby语句 | groupby语句 | String | ✓ |  |
| selectClause | select语句 | select语句 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码

```python
data = data.link(GroupByBatchOp().setGroupByPredicate("f1").setSelectClause("f1,avg(f2) as f2"))
```
