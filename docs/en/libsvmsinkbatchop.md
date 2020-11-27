## Description
Sink the data to files in libsvm format.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |
| overwriteSink | Whether to overwrite existing data. | Boolean |  | false |
| vectorCol | Name of a vector column | String | ✓ |  |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| startIndex | start index | Integer |  | 1 |

## Script Example

### Code

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
