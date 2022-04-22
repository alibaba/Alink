# 推荐负采样 (NegativeItemSamplingBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.NegativeItemSamplingBatchOp

Python 类名：NegativeItemSamplingBatchOp


## 功能介绍
当给定user-item pair数据的时候，为数据生成若干负样本数据，构成训练数据。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| samplingFactor | 采样因子 | 采样因子 | Integer |  |  | 3 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
     [1, 1],
     [2, 2],
     [2, 3],
     [4, 1],
     [4, 2],
     [4, 3],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint')

NegativeItemSamplingBatchOp().linkFrom(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.NegativeItemSamplingBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NegativeItemSamplingBatchOpTest {
	@Test
	public void testNegativeItemSamplingBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1),
			Row.of(2, 2),
			Row.of(2, 3),
			Row.of(4, 1),
			Row.of(4, 2),
			Row.of(4, 3)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int");
		new NegativeItemSamplingBatchOp().linkFrom(data).print();
	}
}
```

### 运行结果
user|item|label
----|----|-----
2|1|0
1|3|0
4|1|1
4|2|1
1|3|0
2|1|0
2|1|0
4|3|1
2|2|1
2|3|1
2|1|0
1|1|1
2|1|0
1|3|0
2|1|0
