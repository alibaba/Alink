# 特征构造：OverCountWindow (OverCountWindowStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp

Python 类名：OverCountWindowStreamOp


## 功能介绍

OverCount窗口是Over窗口的一种，基于OverWindow，使用聚合函数进行流式特征构造。给定一行数据，将生成的特征追加在后面，输出一行数据，生成方式有clause(表达式决定)。

* clause语句的形式，通过聚合函数进行操作。其中clause语法和flink sql一致，计算逻辑也和flink overwindow一致。
* 依据指定列进行groupBy，在用户指定的窗口区间内，按照clause指定的方式进行计算。

### Clause
clause当前支持全部flink支持的聚合函数，并在此基础上额外支持了一系列聚合函数。

详细用法请参考 http://alinklab.cn/tutorial/appendix_aggregate_function.html

### 窗口

Alink支持的窗口, 其中Group窗口是输出窗口聚合统计量，OVER窗口是给定一行数据，将窗口特征追加到数据后面，输出带特征的一行数据。

<div align=center><img src="https://img.alicdn.com/imgextra/i3/O1CN01SYSIhd1ZcW6e3RMo6_!!6000000003215-0-tps-940-489.jpg" height="50%" width="50%"></div>

各窗口的详细用法请参考 https://www.yuque.com/pinshu/alink_guide/dffffm


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ | 所选列类型为 [TIMESTAMP] |  |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| latency | 水位线的延迟 | 水位线的延迟，默认0.0 | Double |  |  | 0.0 |
| precedingRows | 数据窗口大小 | 数据窗口大小 | Integer |  |  | null |
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

op = OverCountWindowStreamOp().setTimeCol("timeCol").setPrecedingRows(10).setGroupCols(["user"]).setClause("count_preceding(ip) as countip")

source.select('user, device, ip, to_timestamp(timeCol) as timeCol').link(op).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.SqlCmdStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OverCountWindowStreamOpTest {
	@Test
	public void testOverCountWindowStreamOp() throws Exception {
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
		StreamOperator <?> op = new OverCountWindowStreamOp().setTimeCol("timeCol").setPrecedingRows(10)
			.setGroupCols("user").setClause("count_preceding(ip) as countip");
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
