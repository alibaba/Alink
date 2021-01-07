## Description
Create a stream with a ak file from file system.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |

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
BatchOperator.execute()

# read ak file and print
AkSourceStreamOp().setFilePath(FilePath(filePath)).print()
StreamOperator.execute()
```

### Result
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
