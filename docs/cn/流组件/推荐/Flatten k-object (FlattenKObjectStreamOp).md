# Flatten k-object (FlattenKObjectStreamOp)
Java 类名：com.alibaba.alink.operator.stream.recommendation.FlattenKObjectStreamOp

Python 类名：FlattenKObjectStreamOp


## 功能介绍
将流式推荐结果从json序列化格式转为table格式。

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
    [1,'{"rating":"[0.6]","object":"[1]"}'],
    [2,'{"rating":"[0.8,0.6]","object":"[2,3]"}'],
    [3,'{"rating":"[0.6,0.3,0.4]","object":"[1,2,3]"}']
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, rec string')
sdata = StreamOperator.fromDataframe(df_data, schemaStr='user bigint, rec string')

recList = FlattenKObjectStreamOp()\
			.setSelectedCol("rec")\
			.setOutputCols(["object", "rating"])\
			.setOutputColTypes(["long", "double"])\
			.setReservedCols(["user"])\
			.linkFrom(sdata).print();
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.recommendation.FlattenKObjectStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FlattenKObjectStreamOpTest {
	@Test
	public void testFlattenKObjectStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, "{\"rating\":\"[0.6]\",\"object\":\"[1]\"}"),
			Row.of(2, "{\"rating\":\"[0.8,0.6]\",\"object\":\"[2,3]\"}"),
			Row.of(3, "{\"rating\":\"[0.6,0.3,0.4]\",\"object\":\"[1,2,3]\"}")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, rec string");
		StreamOperator <?> sdata = new MemSourceStreamOp(df_data, "user int, rec string");
		StreamOperator <?> recList = new FlattenKObjectStreamOp()
			.setSelectedCol("rec")
			.setOutputCols("object", "rating")
			.setOutputColTypes("long", "double")
			.setReservedCols("user")
			.linkFrom(sdata).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
user|object|rating
----|------|------
1|1|0.6000
2|2|0.8000
2|3|0.6000
3|1|0.6000
3|2|0.3000
3|3|0.4000
