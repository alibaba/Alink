# Json值抽取

## 功能介绍

该组件完成json字符串中的信息抽取，按照用户给定的Path 抓取出相应的信息。该组件支持多Path抽取。

## 参数说明


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| jsonPath | Json 路径数组 | 用来指定 Json 抽取的内容。 | String[] | ✓ |  |
| skipFailed | 是否跳过错误 | 当遇到抽取值为null 时是否跳过 | boolean |  | false |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```python
import numpy as np
import pandas as pd
data = np.array([
    ["{a:boy,b:{b1:1,b2:2}}"],
    ["{a:girl,b:{b1:1,b2:2}}"]])
df = pd.DataFrame({"str": data[:, 0]})

streamData = dataframeToOperator(df, schemaStr='str string', op_type='stream')

JsonValueStreamOp().setJsonPath(["$.a","$.b.b1"]).setSelectedCol("str").setOutputCols(["f0","f1"]).linkFrom(streamData).print()
StreamOperator.execute()
```

#### 运行结果

str | f0 | f1
----|----|---
{a:boy,b:{b1:1,b2:2}}|boy|1
{a:girl,b:{b1:1,b2:2}}|girl|1





