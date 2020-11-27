# 读CSV文件

## 功能介绍
读LibSVM文件。支持从本地、hdfs读取。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| startIndex | 起始索引 | 起始索引 | Integer |  | 1 |


## 脚本示例

### 脚本代码

```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

data = {
  'f1': ['1:2.0 2:1.0 4:0.5','1:2.0 2:1.0 4:0.5','1:2.0 2:1.0 4:0.5'],
  'f2': [1.5, 1.7, 3.6]
}
df_data = pd.DataFrame(data)
schema = 'f1 string, f2  double'
batch_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
filepath = '/tmp/abc.svm'

sink = LibSvmSinkBatchOp().setFilePath(filepath).setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
batch_data = batch_data.link(sink)

BatchOperator.execute()

batch_data = LibSvmSourceBatchOp().setFilePath(filepath)
batch_data.print()

```
