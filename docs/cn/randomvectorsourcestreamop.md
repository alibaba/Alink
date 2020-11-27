# 读随机张量(Stream)

## 功能介绍
生成随机张量的表

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| idCol | id 列名 | 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列 | String |  | "alink_id" |
| outputCol | 输出列名 | 输出随机生成的数据存储列名 | String |  | "tensor" |
| size | 张量size | 整型数组，张量的size | Integer[] | ✓ |  |
| maxRows | 最大行数 | 输出数据流的行数目的最大值 | Long | ✓ |  |
| sparsity | 稀疏度 | 非零元素在所有张量数据中的占比 | Double | ✓ |  |
| timePerSample | 稀疏度 | 整型数组，张量的size | Double |  | null |


## 脚本示例
### 脚本代码
```python
from pyalink.alink import *
RandomVectorSourceStreamOp().setMaxRows(5).setSize([2]).setSparsity(1.0).print()
StreamOperator.execute()
```
### 运行结果
   alink_id                                         tensor
        0   $2$0:0.6374174253501083 1:0.5504370051176339
        2                        $2$0:0.3851891847407185
        4  $2$0:0.9412491794821144 1:0.27495396603548483
        1                       $2$0:0.20771484130971707
        3                        $2$1:0.7107396275716601


