## Description
Support SQL select statements which can be used in Pipeline.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| clause | Operation clause. | String | ✓ |  |


## Script Example
#### Code

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

