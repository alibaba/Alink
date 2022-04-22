# 展开KObject (FlattenKObjectBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.FlattenKObjectBatchOp

Python 类名：FlattenKObjectBatchOp


## 功能介绍
将推荐结果从json序列化格式转为table格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| outputColTypes | 输出结果列列类型数组 | 输出结果列类型数组 | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

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
    [4, 1, 0.6],
    [4, 2, 0.3],
    [4, 3, 0.4],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')

jsonData = Zipped2KObjectBatchOp()\
			.setGroupCol("user")\
            .setObjectCol("item")\
			.setInfoCols(["rating"])\
			.setOutputCol("recomm")\
			.linkFrom(data)\
			.lazyPrint(-1);
recList = FlattenKObjectBatchOp()\
			.setSelectedCol("recomm")\
			.setOutputCols(["item", "rating"])\
    		.setOutputColTypes(["long", "double"])\
			.setReservedCols(["user"])\
			.linkFrom(jsonData)\
			.lazyPrint(-1);
BatchOperator.execute();
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.FlattenKObjectBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.recommendation.Zipped2KObjectBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FlattenKObjectBatchOpTest {
	@Test
	public void testFlattenKObjectBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> jsonData = new Zipped2KObjectBatchOp()
			.setGroupCol("user")
			.setObjectCol("item")
			.setInfoCols("rating")
			.setOutputCol("recomm")
			.linkFrom(data)
			.lazyPrint(-1);
		BatchOperator <?> recList = new FlattenKObjectBatchOp()
			.setSelectedCol("recomm")
			.setOutputCols("item", "rating")
			.setOutputColTypes("long", "double")
			.setReservedCols("user")
			.linkFrom(jsonData)
			.lazyPrint(-1);
		BatchOperator.execute();
	}
}
```

### 运行结果
user|recomm
----|------
1|{"item":"[1]","rating":"[0.6]"}
4|{"item":"[1,2,3]","rating":"[0.6,0.3,0.4]"}
2|{"item":"[2,3]","rating":"[0.8,0.6]"}

user|item|rating
----|----|------
1|1|0.6000
4|1|0.6000
4|2|0.3000
4|3|0.4000
2|2|0.8000
2|3|0.6000
