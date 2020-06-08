## 功能介绍
提供可以在 Pipeline 中使用 SQL select 语句的功能。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码

```python
import pandas as pd
import numpy as np

schema = "age int, name string"

data = np.array([
    [14, "Tony"],
    [35, "Tommy"],
    [72, "Tongli"],
])

df = pd.DataFrame.from_records(data)
source = BatchOperator.fromDataframe(df, "age int, name string")

select = Select().setClause("CASE WHEN age < 18 THEN 0 WHEN age >= 18 AND age < 60 THEN 1 ELSE 2 END AS class, name")

select.transform(source).print()
```
