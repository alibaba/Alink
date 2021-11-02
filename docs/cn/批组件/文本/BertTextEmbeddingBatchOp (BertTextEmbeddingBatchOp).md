# BertTextEmbeddingBatchOp (BertTextEmbeddingBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.BertTextEmbeddingBatchOp

Python 类名：BertTextEmbeddingBatchOp


## 功能介绍

把文本输入到 BERT 模型，提取某一编码层的 pooled output 作为该句子的 embedding 结果。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| modelPath | 模型的URL路径 | 模型的URL路径 | String | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  | "Base-Chinese" |
| doLowerCase | 是否将文本转换为小写 | 是否将文本转换为小写，默认根据模型自动决定 | Boolean |  | null |
| intraOpParallelism | python进程使用的线程数 | python进程使用的线程数 | Integer |  | 4 |
| layer | 输出第几层encoder layer | 输出第几层encoder layer， -1 表示最后一层，-2 表示倒数第2层，以此类推 | Integer |  | -1 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  | 128 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)


downloader = AlinkGlobalConfiguration.getPluginDownloader()
downloader.downloadPlugin("tf_predictor_macosx")
downloader.downloadPlugin("base_chinese_saved_model")

df_data = pd.DataFrame([
    ['An english sentence.', 1],
    ['这是一个中文句子', 2]
])

batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint')

BertTextEmbeddingBatchOp()
    .setSelectedCol("f1")
    .setOutputCol("embedding")
    .setLayer(-2)
    .linkFrom(batch_data)
    .print()
```

### 运行结果

```
                     f1  f2                                          embedding
0  An english sentence.   1  -0.4501993 0.06074004 0.121287264 -0.27875 0.3...
1         这是一个中文句子   2  -0.8317032 0.32284066 -0.12233654 -0.6955824 0...
```
