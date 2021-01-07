## Description
Stream source that reads reads data with tab-separated values.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |
| schemaStr | Formatted schema | String | ✓ |  |
| skipBlankLine | skipBlankLine | Boolean |  | true |
| ignoreFirstLine | Whether to ignore first line of csv file. | Boolean |  | false |

## Script Example

### Code

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

source = dataframeToOperator(df, schemaStr='uid string, iid string, label double', op_type='stream')

filepath = '*'
tsvSink = TsvSinkStreamOp()\
    .setFilePath(filepath)

source.link(tsvSink)

StreamOperator.execute()

tsvSource = TsvSourceStreamOp().setFilePath(filepath).setSchemaStr("f string");
tsvSource.print()

StreamOperator.execute()


```
