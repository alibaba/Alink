## 功能介绍

输出文章列所有词以及对应的词频，其中词由文章列内容根据分隔符划分产生。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  | " " |

## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
records = [
    ("doc0", "中国 的 文化"),
    ("doc1", "只要 功夫 深"),
    ("doc2", "北京 的 拆迁"),
    ("doc3", "人名 的 名义")
]
df = pd.DataFrame.from_records(records)
source = BatchOperator.fromDataframe(df, "id string, content string")
wordCountBatchOp = WordCountBatchOp()\
    .setSelectedCol("content")\
    .setWordDelimiter(" ")\
    .linkFrom(source)
wordCountBatchOp.print()
```

#### 脚本运行结果
word|cnt
----|---
人名|1
北京|1
只要|1
名义|1
文化|1
中国|1
功夫|1
拆迁|1
深|1
的|3
