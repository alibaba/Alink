## Description
Sink batch op data to a file system with ak format.
 Ak is a file format define by alink.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |
| overwriteSink | Whether to overwrite existing data. | Boolean |  | false |
| numFiles | Number of files | Integer |  | 1 |

## Script Example

### Code
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
