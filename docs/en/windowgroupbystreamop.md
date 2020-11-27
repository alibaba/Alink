## Description
A wrapper of Flink's window groupby.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectClause | Select clause | String | ✓ |  |
| groupByClause | Groupby clause | String |  | null |
| intervalUnit | Interval unit, one of second, minute, hour, day or month | String |  | "SECOND" |
| windowType | Window type, one of "tumble", "hop", "session" | String |  | "TUMBLE" |
| sessionGap | Session gap | Integer | ✓ |  |
| slidingLength | Sliding length | Integer | ✓ |  |
| windowLength | Window length | Integer | ✓ |  |

## Script Example

### Code

```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

data = {
  'f1': ['changjiang', 'huanghe', 'zhujiang', 'changjiang', 'huanghe', 'zhujiang'],
  'f2': [2000, 2001, 2002, 2001, 2002, 2003],
  'f3': [1.5, 1.7, 3.6, 2.4, 2.9, 3.2]
}
df_data = pd.DataFrame(data)
schema = 'f1 string, f2 bigint, f3 double'
stream_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

op = WindowGroupByStreamOp() \
  .setGroupByClause("f1") \
  .setSelectClause("sum(f2) as f2, f1").setWindowLength(1)
stream_data = stream_data.link(op)

stream_data.print()
StreamOperator.execute()
resetEnv()

```

### Results

```
['f2', 'f1', 'window_start', 'window_end']

```
