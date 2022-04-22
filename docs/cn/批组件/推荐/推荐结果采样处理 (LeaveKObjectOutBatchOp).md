# 推荐结果采样处理 (LeaveKObjectOutBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.LeaveKObjectOutBatchOp

Python 类名：LeaveKObjectOutBatchOp


## 功能介绍
将推荐结果分成两个表。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| groupCol | 分组列 | 分组单列名，必选 | String | ✓ |  |  |
| objectCol | Object列列名 | Object列列名 | String | ✓ |  |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| fraction | 拆分到测试集最大数据比例 | 拆分到测试集最大数据比例 | Double |  | [0.0, 1.0] | 1.0 |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  |  | 10 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    [1, 1, 0.6],
    [2, 2, 0.8],
    [2, 3, 0.6],
    [4, 0, 0.6],
    [6, 4, 0.3],
    [4, 7, 0.4],
    [2, 6, 0.6],
    [4, 5, 0.6],
    [4, 6, 0.3],
    [4, 3, 0.4]
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')

spliter = LeaveKObjectOutBatchOp()\
			.setK(2)\
			.setGroupCol("user")\
			.setObjectCol("item")\
			.setOutputCol("label")
spliter.linkFrom(data).print()
spliter.getSideOutput(0).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.LeaveKObjectOutBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LeaveKObjectOutBatchOpTest {
	@Test
	public void testLeaveKObjectOutBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 0, 0.6),
			Row.of(6, 4, 0.3),
			Row.of(4, 7, 0.4),
			Row.of(2, 6, 0.6),
			Row.of(4, 5, 0.6),
			Row.of(4, 6, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> spliter = new LeaveKObjectOutBatchOp()
			.setK(2)
			.setGroupCol("user")
			.setObjectCol("item")
			.setOutputCol("label");
		spliter.linkFrom(data).print();
		spliter.getSideOutput(0).print();
	}
}
```

### 运行结果
user|label
----|-----
1|{"item":"[1]"}
6|{"item":"[4]"}
4|{"item":"[7,3]"}
2|{"item":"[2,6]"}

user|item|rating
----|----|------
4|5|0.6000
4|3|0.4000
4|0|0.6000
2|3|0.6000
