# 推荐结果TopK采样处理 (LeaveTopKObjectOutBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.LeaveTopKObjectOutBatchOp

Python 类名：LeaveTopKObjectOutBatchOp


## 功能介绍
将推荐结果按取topK部分作为一个输出表。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| groupCol | 分组列 | 分组单列名，必选 | String | ✓ |  |  |
| objectCol | Object列列名 | Object列列名 | String | ✓ |  |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| fraction | 拆分到测试集最大数据比例 | 拆分到测试集最大数据比例 | Double |  | [0.0, 1.0] | 1.0 |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  |  | 10 |
| rateThreshold | 打分阈值 | 打分阈值 | Double |  |  | -Infinity |

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

spliter = LeaveTopKObjectOutBatchOp()\
			.setK(2)\
			.setGroupCol("user")\
			.setObjectCol("item")\
			.setOutputCol("label")\
            .setRateCol("rating")
spliter.linkFrom(data).print()
spliter.getSideOutput(0).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.LeaveTopKObjectOutBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LeaveTopKObjectOutBatchOpTest {
	@Test
	public void testLeaveTopKObjectOutBatchOp() throws Exception {
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
		BatchOperator <?> spliter = new LeaveTopKObjectOutBatchOp()
			.setK(2)
			.setGroupCol("user")
			.setObjectCol("item")
			.setOutputCol("label")
			.setRateCol("rating");
		spliter.linkFrom(data).print();
		spliter.getSideOutput(0).print();
	}
}
```

### 运行结果
user|label
----|-----
1|{"item":"[1]","rating":"[0.6]"}
6|{"item":"[4]","rating":"[0.3]"}
4|{"item":"[0,5]","rating":"[0.6,0.6]"}
2|{"item":"[2,3]","rating":"[0.8,0.6]"}

user|item|rating
----|----|------
4|7|0.4000
4|3|0.4000
4|6|0.3000
2|6|0.6000
