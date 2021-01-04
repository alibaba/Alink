# 读Tsv

## 功能介绍

读Tsv文件，Tsv文件是以tab为分隔符

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| skipBlankLine | 是否忽略空行 | 是否忽略空行 | Boolean |  | true |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  | false |


## 脚本示例

### 脚本代码

#### Derby
```python
import numpy as np
import pandas as pd

data = np.array([
                ["0L", "1L", 0.6],
                ["2L", "2L", 0.8],
                ["2L", "4L", 0.6],
                ["3L", "1L", 0.6],
                ["3L", "2L", 0.3],
                ["3L", "4L", 0.4]
        ])
df = pd.DataFrame({"uid": data[:, 0], "iid": data[:, 1], "label": data[:, 2]})

source = dataframeToOperator(df, schemaStr='uid string, iid string, label double', op_type='batch')

filepath = '*'
tsvSink = TsvSinkBatchOp()\
    .setFilePath(filepath)

source.link(tsvSink)

BatchOperator.execute()

tsvSource = TsvSourceBatchOp().setFilePath(filepath).setSchemaStr("f string");
tsvSource.print()

```
