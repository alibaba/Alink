# 随机生成向量数据源 (RandomVectorSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.RandomVectorSourceStreamOp

Python 类名：RandomVectorSourceStreamOp


## 功能介绍
生成随机张量的表

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| maxRows | 最大行数 | 输出数据流的行数目的最大值 | Long | ✓ |  |  |
| size | 张量size | 整型数组，张量的size | Integer[] | ✓ |  |  |
| sparsity | 稀疏度 | 非零元素在所有张量数据中的占比 | Double | ✓ |  |  |
| idCol | id 列名 | 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列 | String |  |  | "alink_id" |
| outputCol | 输出列名 | 输出随机生成的数据存储列名 | String |  |  | "tensor" |
| timePerSample | 稀疏度 | 整型数组，张量的size | Double |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

RandomVectorSourceStreamOp().setMaxRows(5).setSize([2]).setSparsity(1.0).print()
StreamOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.RandomVectorSourceStreamOp;
import org.junit.Test;

public class RandomVectorSourceStreamOpTest {
	@Test
	public void testRandomVectorSourceStreamOp() throws Exception {
		new RandomVectorSourceStreamOp().setMaxRows(5L).setSize(new Integer[] {2}).setSparsity(1.0).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
alink_id|tensor
--------|------
5|$2$0:0.4889045498516358 1:0.461837214623537
1|$2$0:0.20771484130971707
3|$2$0:0.06712000939049956 1:0.768156984078079
4|$2$1:0.9186071189908658
2|$2$0:0.49682259343089075 1:0.9858769332362016

