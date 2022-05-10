# 内存数据源 (MemSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.MemSourceStreamOp

Python 类名：MemSourceStreamOp


## 功能介绍
从内存中读取数据生成表。MemSourceStreamOp支持多个构造函数：

|构造函数|参数| 示例 |
|:------|:----|:----|
|MemSourceStreamOp(Object[] vals, String colName)|数据只有一列，列类型从数据判断| MemSourceStreamOp(new Object[]{1.0, 2.0}, "f0")
|MemSourceStreamOp(Object[][] vals, String[] colNames)|colNames是列名列表，列类型从数据判断|MemSourceStreamOp(new Object[][]{{1.0, 2.0}, {3.0, 4.0}}, new String[]{"f0", "f1"})
|MemSourceStreamOp(List <Row> rows, TableSchema schema)|schema|MemSourceStreamOp(df, new TableSchema(new String[]{"f1", "f2"}, new TypeInformation[]{Types.STRING, Types.DOUBLE}))
|MemSourceStreamOp(List <Row> rows, String schemaStr)|schemaStr格式是col1 string, f1 int...|MemSourceStreamOp(df, "f1 string, f2  double")
|MemSourceStreamOp(Row[] rows, String[] colNames)|colNames是列名列表，列类型从数据判断|MemSourceStreamOp(rows, new String[]{"f0", "f1"})
|MemSourceStreamOp(List <Row> rows, String[] colNames)|colNames是列名列表，列类型从数据判断|MemSourceStreamOp(rows, new String[]{"f0", "f1"})


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
无，仅在Java中使用


### Java 代码

```java
package javatest.com.alibaba.alink.stream.source;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MemSourceStreamOpTest {
	@Test
	public void testMemSourceStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:2.0 2:1.0 4:0.5", 1.5),
			Row.of("1:2.0 2:1.0 4:0.5", 1.7),
			Row.of("1:2.0 2:1.0 4:0.5", 3.6)
		);
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "f1 string, f2  double");
		streamData.print();
		StreamOperator.execute();
	}
}

```

### 运行结果
f1|f2
---|---
1:2.0 2:1.0 4:0.5|1.7000
1:2.0 2:1.0 4:0.5|1.5000
1:2.0 2:1.0 4:0.5|3.6000
