# 特征构造：OverTimeWindow (OverTimeWindowStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.OverTimeWindowStreamOp

Python 类名：OverTimeWindowStreamOp


## 功能介绍

基于flink OverWindow，使用聚合函数进行流式特征构造。

* clause语句的形式，通过聚合函数进行操作。其中clause语法和flink sql一致，计算逻辑也和flink overwindow一致。
* 依据指定列进行groupBy，在用户指定的窗口区间内，按照clause指定的方式进行计算。

### Clause
clause当前支持全部flink支持的聚合函数，并在此基础上额外支持了一系列聚合函数。

详细用法请参考 https://www.yuque.com/pinshu/alink_tutorial/list_aggregate_function


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ | 所选列类型为 [TIMESTAMP] |  |
| latency | 水位线的延迟 | 水位线的延迟，默认0.0 | Double |  |  | 0.0 |
| partitionCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| precedingTime | 时间窗口大小 | 时间窗口大小 | Double |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| watermarkType | 水位线的类别 | 水位线的类别 | String |  | "PERIOD", "PUNCTUATED" | "PERIOD" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

sourceFrame = pd.DataFrame([
        [0, 0, 0, 1],
        [0, 2, 0, 2],
        [0, 1, 1, 3],
        [0, 3, 1, 4],
        [0, 3, 3, 5],
        [0, 0, 3, 6],
        [0, 0, 4, 7],
        [0, 3, 4, 8],
        [0, 1, 2, 9],
        [0, 2, 2, 10],
    ])

source = StreamOperator.fromDataframe(sourceFrame,schemaStr="user int, device long, ip long, timeCol long")

op = OverTimeWindowStreamOp().setTimeCol("timeCol").setPrecedingTime(10.0).setPartitionCols(["user"]).setClause("count_preceding(ip) as countip")

source.select('user, device, ip, to_timestamp(timeCol) as timeCol').link(op).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.SqlCmdStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OverTimeWindowStreamOpTest {
	@Test
	public void testOverTimeWindowStreamOp() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of(0, 0, 0, 1L),
			Row.of(0, 2, 0, 2L),
			Row.of(0, 1, 1, 3L),
			Row.of(0, 3, 1, 4L),
			Row.of(0, 3, 3, 5L),
			Row.of(0, 0, 3, 6L),
			Row.of(0, 0, 4, 7L),
			Row.of(0, 3, 4, 8L),
			Row.of(0, 1, 2, 9L),
			Row.of(0, 2, 2, 10L)
		);
		StreamOperator <?> streamSource = new MemSourceStreamOp(sourceFrame,
			"user int, device int, ip int, timeCol long");
		StreamOperator <?> op = new OverTimeWindowStreamOp().setTimeCol("timeCol").setPrecedingTime(10.0)
			.setPartitionCols("user").setClause("count_preceding(ip) as countip");
		streamSource.select("user, device, ip, to_timestamp(timeCol) as timeCol").link(op).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

user|device|ip|timeCol|countip
----|------|---|-------|-------
0|0|0|1970-01-01 08:00:00.001|0
0|2|0|1970-01-01 08:00:00.002|1
0|1|1|1970-01-01 08:00:00.003|2
0|3|1|1970-01-01 08:00:00.004|3
0|3|3|1970-01-01 08:00:00.005|4
0|0|3|1970-01-01 08:00:00.006|5
0|0|4|1970-01-01 08:00:00.007|6
0|3|4|1970-01-01 08:00:00.008|7
0|1|2|1970-01-01 08:00:00.009|8
0|2|2|1970-01-01 08:00:00.01|9
