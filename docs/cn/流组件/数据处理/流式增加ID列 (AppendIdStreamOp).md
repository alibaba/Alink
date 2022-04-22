# 流式增加ID列 (AppendIdStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.AppendIdStreamOp

Python 类名：AppendIdStreamOp


## 功能介绍

将表附加ID列

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| idCol | ID列名 | ID列名 | String |  |  | "append_id" |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003,3.2]
])

stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')

AppendIdStreamOp().linkFrom(stream_data).print()

StreamOperator.execute()
```

### 运行结果

```
['f1', 'f2', 'f3', 'append_id']
['Ohio', 2000, 1.5, 0]
['Ohio', 2001, 1.7, 1]
['Ohio', 2002, 3.6, 2]
['Nevada', 2001, 2.4, 3]
['Nevada', 2002, 2.9, 4]
['Nevada', 2003, 3.2, 5]
```


