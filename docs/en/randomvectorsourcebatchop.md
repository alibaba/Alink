## Description
Generate vector with random values.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| idCol | id col name | String |  | "alink_id" |
| outputCol | output col name | String |  | "tensor" |
| numRows | num rows | Integer | ✓ |  |
| size | size | Integer[] | ✓ |  |
| sparsity | sparsity | Double | ✓ |  |

## Script Example
### Code
```python
from pyalink.alink import *
RandomVectorSourceBatchOp().setNumRows(5).setSize([2]).setSparsity(1.0).print()
```
### Result
   alink_id                                         tensor
0         0   $2$0:0.6374174253501083 1:0.5504370051176339
1         2                        $2$0:0.3851891847407185
2         4  $2$0:0.9412491794821144 1:0.27495396603548483
3         1                       $2$0:0.20771484130971707
4         3                        $2$1:0.7107396275716601


