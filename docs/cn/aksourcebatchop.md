# 读Ak文件

## 功能介绍
从文件系统读Ak文件。Ak文件格式是Alink 自定义的一种文件格式，能够将数据的Schema保留输出的文件格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |

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
BatchOperator.execute()

# read ak file and print
AkSourceBatchOp().setFilePath(FilePath(filePath)).print()
```

### 运行结果
```
	f0	f1	label
0	2	1	1
1	3	2	1
2	4	3	2
3	2	4	1
4	2	2	1
5	4	3	2
6	1	2	1
7	5	3	3
```
