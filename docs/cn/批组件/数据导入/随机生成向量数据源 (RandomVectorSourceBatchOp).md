# 随机生成向量数据源 (RandomVectorSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.RandomVectorSourceBatchOp

Python 类名：RandomVectorSourceBatchOp


## 功能介绍
生成随机张量表

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numRows | 输出表行数目 | 输出表中行的数目，整型 | Integer | ✓ |  |
| size | 张量size | 整型数组，张量的size | Integer[] | ✓ |  |
| sparsity | 稀疏度 | 非零元素在所有张量数据中的占比 | Double | ✓ |  |
| idCol | id 列名 | 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列 | String |  | "alink_id" |
| outputCol | 输出列名 | 输出随机生成的数据存储列名 | String |  | "tensor" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

RandomVectorSourceBatchOp().setNumRows(5).setSize([2]).setSparsity(1.0).print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.source.RandomVectorSourceBatchOp;
import org.junit.Test;

public class RandomVectorSourceBatchOpTest {
	@Test
	public void testRandomVectorSourceBatchOp() throws Exception {
		new RandomVectorSourceBatchOp().setNumRows(5).setSize(new Integer[]{2}).setSparsity(1.0).print();
	}
}
```
### 运行结果
alink_id|tensor
--------|------
0|$2$0:0.6374174253501083 1:0.5504370051176339
1|$2$0:0.20771484130971707
2|$2$0:0.49682259343089075 1:0.9858769332362016
3|$2$0:0.06712000939049956 1:0.768156984078079
4|$2$1:0.9186071189908658
