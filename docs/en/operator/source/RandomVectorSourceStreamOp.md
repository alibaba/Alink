## Description
Generate vector with random values.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| idCol | id col name | String |  | "alink_id" |
| outputCol | output col name | String |  | "tensor" |
| size | size | Integer[] | ✓ |  |
| maxRows | max rows | Long | ✓ |  |
| sparsity | sparsity | Double | ✓ |  |
| timePerSample | time per sample | Double |  | null |

## Script Example
### Code
```python
from pyalink.alink import *
RandomVectorSourceStreamOp().setMaxRows(5).setSize([2]).setSparsity(1.0).print()
StreamOperator.execute()
```
### Result
   alink_id                                         tensor
        0   $2$0:0.6374174253501083 1:0.5504370051176339
        2                        $2$0:0.3851891847407185
        4  $2$0:0.9412491794821144 1:0.27495396603548483
        1                       $2$0:0.20771484130971707
        3                        $2$1:0.7107396275716601


