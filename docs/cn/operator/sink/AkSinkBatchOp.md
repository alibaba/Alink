# 写Ak文件

## 功能介绍
将一个批式数据，以Ak文件格式写出到文件系统。Ak文件格式是Alink 自定义的一种文件格式，能够将数据的Schema保留输出的文件格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |
| numFiles | 文件数目 | 文件数目 | Integer |  | 1 |

## 脚本示例

### 脚本代码
```python
import numpy as np
import pandas as pd
from pyalink.alink import *

data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})
batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

filePath = "/tmp/test_alink_file_sink";

# write file to local disk
batchData.link(AkSinkBatchOp()\
				.setFilePath(FilePath(filePath))\
				.setOverwriteSink(True)\
				.setNumFiles(1))

# write file to hadoop file system
hdfsFilePath = "alink_fs_test/test_alink_file_sink";
fs = HadoopFileSystem("2.8.3", "hdfs://10.101.201.169:9000");
batchData.link(AkSinkBatchOp()\
				.setFilePath(FilePath(hdfsFilePath, fs))\
				.setOverwriteSink(True)\
				.setNumFiles(1))

BatchOperator.execute()
```
