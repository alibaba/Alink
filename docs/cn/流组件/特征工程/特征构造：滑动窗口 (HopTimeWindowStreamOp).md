# 特征构造：滑动窗口 (HopTimeWindowStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp

Python 类名：HopTimeWindowStreamOp


## 功能介绍

滑动窗口是GroupWindow的一种，基于GroupWindow，使用聚合函数进行计算，输出窗口内的统计量，特征生成方式由clause(表达式决定)。

* clause语句的形式，通过聚合函数进行操作。其中clause语法和flink sql一致，计算逻辑也和flink overwindow一致。
* 依据指定列进行groupBy，在用户指定的窗口区间内，按照clause指定的方式进行计算。

### Clause
clause当前支持全部flink支持的聚合函数，并在此基础上额外支持了一系列聚合函数。

详细用法请参考 https://www.yuque.com/pinshu/alink_tutorial/list_aggregate_function

### 窗口

Alink支持的窗口, 其中Group窗口是输出窗口聚合统计量，OVER窗口是给定一行数据，将窗口特征追加到数据后面，输出带特征的一行数据。

<div align=center><img src="https://img.alicdn.com/imgextra/i3/O1CN01SYSIhd1ZcW6e3RMo6_!!6000000003215-0-tps-940-489.jpg" height="50%" width="50%"></div>

各窗口的详细用法请参考 https://www.yuque.com/pinshu/alink_guide/dffffm


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |
| hopTime | 滑动窗口大小 | 滑动窗口大小 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ | 所选列类型为 [TIMESTAMP] |  |
| windowTime | 窗口大小 | 窗口大小 | String | ✓ |  |  |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| latency | 水位线的延迟 | 水位线的延迟，默认0.0 | Double |  |  | 0.0 |
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

streamSource = StreamOperator.fromDataframe(sourceFrame,schemaStr="user int, device long, ip long, timeCol long")

op = HopTimeWindowStreamOp()\
        .setTimeCol("timeCol")\
        .setHopTime(40)\
        .setWindowTime(120)\
        .setGroupCols(["user"])\
        .setClause("count_preceding(ip) as countip")

streamSource.select('user, device, ip, to_timestamp(timeCol) as timeCol').link(op).print()

StreamOperator.execute()

```
### Java 代码
```java
package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class HopTimeWindowStreamOpTest extends AlinkTestBase {
	@Test
	public void test() {
		List <Row> sourceFrame = Arrays.asList(
			Row.of(0, 0, 0, new Timestamp(1000L)),
			Row.of(0, 2, 0, new Timestamp(2000L)),
			Row.of(0, 1, 1, new Timestamp(3000L)),
			Row.of(0, 3, 1, new Timestamp(4000L)),
			Row.of(0, 3, 3, new Timestamp(5000L)),
			Row.of(0, 0, 3, new Timestamp(6000L)),
			Row.of(0, 0, 4, new Timestamp(7000L)),
			Row.of(0, 3, 4, new Timestamp(8000L)),
			Row.of(0, 1, 2, new Timestamp(9000L)),
			Row.of(0, 2, 2, new Timestamp(10000L))
		);
		StreamOperator <?> source = new MemSourceStreamOp(
			sourceFrame, new String[] {"user", "device", "ip", "ts"});

		source.link(
			new HopTimeWindowStreamOp()
				.setTimeCol("ts")
				.setHopTime("2s")
				.setWindowTime("5s")
				.setGroupCols("user")
				.setClause("HOP_START() as start_time, HOP_END() as end_time, count_preceding(ip) as countip")
		).print();

		StreamOperator.execute();
	}
}
```

### 运行结果

user|countip
----|-------
0|9
0|9
0|9
