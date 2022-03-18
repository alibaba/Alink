# 特征构造：滑动窗口 (HopTimeWindowStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp

Python 类名：HopTimeWindowStreamOp


## 功能介绍

基于flink GroupWindow，使用聚合函数进行流式特征构造。

* clause语句的形式，通过聚合函数进行操作。其中clause语法和flink sql一致，计算逻辑也和flink overwindow一致。
* 依据指定列进行groupBy，在用户指定的窗口区间内，按照clause指定的方式进行计算。

### Clause
clause当前支持全部flink支持的聚合函数，并在此基础上额外支持了一系列聚合函数。

具体用法如下：


| 聚合操作 | 表达式 | 含义 | 例子 |
| :--- | --- | --- | --- |
| count | count([all &#124; distinct] col) | 计算窗口内数据条数 | count(value) as result |
| count_preceding | count_preceding([all &#124; distinct] col) | 计算窗口内数据条数（不包括当前值） | count_preceding(value) as result |
| sum | sum([all &#124; distinct] col) | 计算窗口内指定列的和 | sum(value) as result |
| sum_preceding | sum_preceding([all &#124; distinct] col) | 计算窗口内指定列的和（不包括当前值） | sum_preceding(value) as result |
| avg | avg([all &#124; distinct] col) | 计算窗口内指定列的均值 | avg(value) as result |
| avg_preceding | avg_preceding([all &#124; distinct] col) | 计算窗口内指定列的均值（不包括当前值） | avg_preceding(value) as result |
| min | min([all &#124; distinct] col) | 计算窗口内指定列的最小值 | min(value) as result |
| min_preceding | min_preceding([all &#124; distinct] col) | 计算窗口内指定列的最小值（不包括当前值） | min_preceding(value) as result |
| max | max([all &#124; distinct] col) | 计算窗口内指定列的最大值 | max(value) as result |
| max_preceding | max_preceding([all &#124; distinct] col) | 计算窗口内指定列的最大值（不包括当前值） | max_preceding(value) as result |
| stddev_samp | stddev_samp([all &#124; distinct] col) | 计算窗口内指定列的样本标准差 | stddev_samp(value) as result |
| stddev_samp_preceding | stddev_samp_preceding([all &#124; distinct] col) | 计算窗口内指定列的样本标准差（不包括当前值） | stddev_samp_preceding(value) as result |
| stddev_pop | stddev_pop([all &#124; distinct] col) | 计算窗口内指定列的总体标准差 | stddev_pop(value) as result |
| stddev_pop_preceding | stddev_pop_preceding([all &#124; distinct] col) | 计算窗口内指定列的总体标准差（不包括当前值） | stddev_pop_preceding(value) as result |
| var_samp | var_samp([all &#124; distinct] col) | 计算窗口内指定列的样本方差 | var_samp(value) as result |
| var_samp_preceding | var_samp_preceding([all &#124; distinct] col) | 计算窗口内指定列的样本方差（不包括当前值） | var_samp_preceding(value) as result |
| var_pop | var_pop([all &#124; distinct] col) | 计算窗口内指定列的总体方差 | var_pop(value) as result |
| var_pop_preceding | var_pop_preceding([all &#124; distinct] col) | 计算窗口内指定列的总体方差（不包括当前值） | var_pop_preceding(value) as result |
| rank | rank() | 计算当前数据在窗口内的排名，生成序号不连续 | rank(value) as result |
| dense_rank | dense_rank()  | 计算当前数据在窗口内的排名，生成序号连续 | dense_rank(value) as result |
| lag | lag(expression [, offset] [, default]) | 计算当前值前offset个值，如果没有返回default | lag(value) as result |
| last_distinct | last_distinct() | 返回窗口内上一个与当前值不同的值 | last_distinct(value) as result |
| last_time | last_time() | 返回窗口内上一个数据的时间 | last_time(value) as result |
| last_value | last_value() | 返回窗口内上一条数据 | last_value(value) as result |
| listagg | listagg(value [, delimiter]) | 返回窗口内数据的拼接值 | listagg(value) as result |
| listagg_preceding | listagg_preceding(value [, delimiter]) | 返回窗口内数据的拼接值（不包括当前值） | listagg_preceding(value) as result |
| mode | mode() | 返回窗口内的众数 | mode(value) as result |
| mode_preceding | mode_preceding() | 返回窗口内的众数（不包括当前值） | mode_preceding(value) as result |
| sum_last | sum_last(value [, k]) | 返回窗口内最近k个值的和 | sum_last(value) as result |
| square_sum | square_sum() | 返回窗口内的平方和 | square_sum(value) as result |
| square_sum_preceding | square_sum_preceding() | 返回窗口内的平方和（不包括当前值） | square_sum_preceding(value) as result |
| median | median() | 返回窗口内的均值 | median(value) as result |
| median_preceding | median_preceding() | 返回窗口内的均值（不包括当前值） | median_preceding(value) as result |
| freq | freq() | 返回当前数据在窗口内出现次数 | freq(value) as result |
| freq_preceding | freq_preceding() | 返回当前数据在窗口内出现次数（不包括当前值） | freq_preceding(value) as result |
| is_exist | is_exist() | 返回当前数据是否在窗口内出现过 | is_exist(value) as result |


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ |  |
| windowTime | 窗口大小 | 窗口大小 | Double | ✓ |  |
| hopTime | 滑动窗口大小 | 滑动窗口大小 | Double | ✓ |  |
| latency | 水位线的延迟 | 水位线的延迟，默认0.0 | Double |  | 0.0 |
| watermarkType | 水位线的类别 | 水位线的类别 | String |  | "PERIOD" |
| partitionCols | 分组列名数组 | 分组列名，多列，可选，必选 | String[] |  | [] |


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

op = HopTimeWindowStreamOp().setTimeCol("timeCol").setHopTime(40).setWindowTime(120).setPartitionCols(["user"]).setClause("count_preceding(ip) as countip")

streamSource.select('user, device, ip, to_timestamp(timeCol) as timeCol').link(op).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.SqlCmdStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HopTimeWindowStreamOpTest {
	@Test
	public void testHopTimeWindowStreamOp() throws Exception {
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
		StreamOperator <?> add_time = new SqlCmdStreamOp().setAlias(new String[] {"time_data"}).setCommand(
			"select user, device, ip, to_timestamp(timeCol) as timeCol from time_data");
		StreamOperator <?> op = new HopTimeWindowStreamOp().setTimeCol("timeCol").setHopTime(40).setWindowTime(120)
			.setPartitionCols("user").setClause("count_preceding(ip) as countip");
		streamSource.link(add_time).link(op).print();
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
