# 流导出到文件 (Export2FileSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.Export2FileSinkStreamOp

Python 类名：Export2FileSinkStreamOp


## 功能介绍
通过本组件可以将流数据方便的转换为分段的批数据。

输出的数据，可以通过 AkSource BatchOp/StreamOp 方便的读取，从而可以完成数据的批和流之间的转换

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| windowTime | 窗口大小 | 窗口大小 | String | ✓ |  |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| partitionsFormat | 分区格式化字符串 | 可以使用类似于 year=yyyy/month=MM/day=dd 的形式自定义分区格式 | String |  |  | null |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码

```python
import time
import datetime
import numpy as np
import pandas as pd

filePath = "/tmp/export_2_file_sink/"

data = pd.DataFrame([
    [0, datetime.datetime.fromisoformat("2021-11-01 00:00:00"), 100.0],
    [0, datetime.datetime.fromisoformat("2021-11-02 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-03 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-04 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-06 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-07 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-08 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-09 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-10 00:00:00"), 900.0],
    [0, datetime.datetime.fromisoformat("2021-11-11 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-12 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-13 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-14 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-15 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-16 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-17 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-18 00:00:00"), 100.0],
    [0, datetime.datetime.fromisoformat("2021-11-19 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-20 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-21 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-22 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-23 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-24 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-25 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-26 00:00:00"), 900.0],
    [0, datetime.datetime.fromisoformat("2021-11-27 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-28 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-29 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-30 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-12-01 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-12-02 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-12-03 00:00:00"), 200.0]
])

streamSource = StreamOperator.fromDataframe(
    data, schemaStr='id int, ts timestamp, val double')

export2FileSinkStreamOp = Export2FileSinkStreamOp()\
    .setFilePath(filePath)\
    .setTimeCol("ts")\
    .setWindowTime(24 * 3600)\
    .setOverwriteSink(True)

streamSource.link(export2FileSinkStreamOp)

StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.Export2FileSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class Export2FileSinkStreamOpTest {

	@Test
	public void testExport2FileSink() throws Exception {
		String filePath = "/tmp/export_2_file_sink/";

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-06 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-07 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-08 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-09 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-10 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-11 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-12 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-13 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-14 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-15 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-16 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-17 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-18 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-19 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-20 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-21 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-22 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-23 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-24 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-25 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-26 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-27 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-28 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-29 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-30 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-12-01 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-12-02 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-12-03 00:00:00"), 200.0)
		);

		StreamOperator <?> streamSource = new MemSourceStreamOp(data, "id int, ts timestamp, val double");

		Export2FileSinkStreamOp export2FileSinkStreamOp = new Export2FileSinkStreamOp()
			.setFilePath(filePath)
			.setTimeCol("ts")
			.setWindowTime(24 * 3600)
			.setOverwriteSink(true);

		streamSource.link(export2FileSinkStreamOp);

		StreamOperator.execute();
	}
}
```
### 运行结果

$ ls /tmp/export_2_file_sink

202111020000000 202111040000000 202111070000000 202111090000000 202111110000000 202111130000000 202111150000000 202111170000000 202111190000000 202111210000000 202111230000000 202111250000000 202111270000000 202111290000000 202112010000000 202112030000000

202111030000000 202111050000000 202111080000000 202111100000000 202111120000000 202111140000000 202111160000000 202111180000000 202111200000000 202111220000000 202111240000000 202111260000000 202111280000000 202111300000000 202112020000000 202112040000000
