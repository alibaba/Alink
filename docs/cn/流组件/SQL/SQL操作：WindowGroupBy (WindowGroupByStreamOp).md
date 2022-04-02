# SQL操作：WindowGroupBy (WindowGroupByStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sql.WindowGroupByStreamOp

Python 类名：WindowGroupByStreamOp


## 功能介绍
对流式数据按窗口做聚合计算。用户可指定窗口的类型、长度，然后通过sql聚合函数对窗口内的数据进行聚合运算，每个窗口输出一条计算结果。若有其它附加的key列，则在每个&lt;窗口，key&gt;内进行聚合运算，输出一条结果。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectClause | select语句 | select语句 | String | ✓ |  |
| sessionGap | session间隔长度 | session间隔长度 | Integer | ✓ |  |
| slidingLength | 滑动窗口滑动长度 | 滑动窗口滑动长度 | Integer | ✓ |  |
| windowLength | 窗口长度 | 窗口长度 | Integer | ✓ |  |
| groupByClause | groupby语句 | groupby语句 | String |  | null |
| intervalUnit | 时间长度单位 | 时间长度单位 | String |  | "SECOND" |
| windowType | 窗口类型 | 窗口类型 | String |  | "TUMBLE" |




### 聚合函数
| SQL语法 | 描述 |
| :--- | :--- |
| COUNT(value [, value]* ) | 返回值不为null 的输入行数。 |
| COUNT(*) | 返回输入行数。 |
| AVG(value) | 返回所有输入值的数值的平均值（算术平均值）。 |
| SUM(value) | 返回所有输入值的数字总和。 |
| MAX(value) | 返回的最大值值在所有的输入值。 |
| MIN(value) | 返回的最小值的值在所有的输入值。 |
| STDDEV_POP(value) | 返回所有输入值的数字字段的总体标准偏差。 |
| STDDEV_SAMP(value) | 返回所有输入值的数字字段的样本标准偏差。 |
| VAR_POP(value) | 返回所有输入值中数字字段的总体方差（总体标准差的平方）。 |
| VAR_SAMP(value) | 返回所有输入值的数值字段的样本方差（样本标准差的平方）。 |
| CONCAT_AGG(value, sep) | sep是分隔符，用指定的spearator做分隔符，连接value中的值。 |


### 注意
不支持 count distinct

### 三种window的区别

参考：[https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html)


<!--
## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003, 3.2]
])

stream_data = StreamOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')

op = WindowGroupByStreamOp() \
  .setGroupByClause("f1") \
  .setSelectClause("sum(f2) as f2, f1").setWindowLength(1)
stream_data = stream_data.link(op)

stream_data.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.WindowGroupByStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WindowGroupByStreamOpTest {
	@Test
	public void testWindowGroupByStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		StreamOperator <?> stream_data = new MemSourceStreamOp(df, "f1 string, f2 int, f3 double");
		StreamOperator <?> op = new WindowGroupByStreamOp()
			.setGroupByClause("f1")
			.setSelectClause("sum(f2) as f2, f1")
            .setWindowLength(1);
		stream_data = stream_data.link(op);
		stream_data.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f2|f1|window_start|window_end
---|---|------------|----------
