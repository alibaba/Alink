## 功能介绍

写出LibSvm格式文件，支持写出到本地文件和HDFS文件。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
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

sink = LibSvmSinkBatchOp().setFilePath('/tmp/abc.svm').setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
batch_data = batch_data.link(sink)

BatchOperator.execute()

```
