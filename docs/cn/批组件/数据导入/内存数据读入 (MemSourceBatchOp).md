# 内存数据读入 (MemSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.MemSourceBatchOp

Python 类名：MemSourceBatchOp


## 功能介绍
从内存中读取数据生成表。

MemSourceBatchOp支持多个构造函数

|构造函数|参数| 示例 |
|:------|:----|:----|
|MemSourceBatchOp(Object[] vals, String colName)|数据只有一列，列类型从数据判断| MemSourceBatchOp(new Object[]{1.0, 2.0}, "f0")
|MemSourceBatchOp(Object[][] vals, String[] colNames)|colNames是列名列表，列类型从数据判断|MemSourceBatchOp(new Object[][]{{1.0, 2.0}, {3.0, 4.0}}, new String[]{"f0", "f1"})
|MemSourceBatchOp(List <Row> rows, TableSchema schema)|schema|MemSourceBatchOp source = new MemSourceBatchOp(df, new TableSchema(new String[]{"f1", "f2"}, new TypeInformation[]{Types.STRING, Types.DOUBLE}))
|MemSourceBatchOp(List <Row> rows, String schemaStr)|schemaStr格式是col1 string, f1 int...|MemSourceBatchOp(df, "f1 string, f2  double")
|MemSourceBatchOp(Row[] rows, String[] colNames)|colNames是列名列表，列类型从数据判断|MemSourceBatchOp(rows, new String[]{"f0", "f1"})
|MemSourceBatchOp(List <Row> rows, String[] colNames)|colNames是列名列表，列类型从数据判断|MemSourceBatchOp(rows, new String[]{"f0", "f1"})

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |




## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
无，仅在Java中使用

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MemSourceBatchOpTest {

	@Test
	public void testMemSourceBatchOp() throws Exception {
		List<Row> df = Arrays.asList(
			Row.of("1:2.0 2:1.0 4:0.5", 1.5),
			Row.of("1:2.0 2:1.0 4:0.5", 1.7),
			Row.of("1:2.0 2:1.0 4:0.5", 3.6)
		);
		BatchOperator<?> batchData = new MemSourceBatchOp(df, "f1 string, f2  double");
		batchData.print();
	}
}

```

### 运行结果

f1|f2
---|---
1:2.0 2:1.0 4:0.5|1.5000
1:2.0 2:1.0 4:0.5|1.7000
1:2.0 2:1.0 4:0.5|3.6000



