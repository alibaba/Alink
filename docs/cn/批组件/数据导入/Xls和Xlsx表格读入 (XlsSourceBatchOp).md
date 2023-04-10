# Xls和Xlsx表格读入 (XlsSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.XlsSourceBatchOp

Python 类名：XlsSourceBatchOp


## 功能介绍
读xls和xlsx后缀名的表格文件，使用时需要下载xls插件。表格文件可能包含的数据类型有string、int、
long、double、float、date、time、datetime、timestamp，来源可以是本地，oss，http，hdfs等。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  |  | false |
| lenient | 是否容错 | 若为true，当解析失败时丢弃该数据；若为false，解析失败是抛异常 | Boolean |  |  | false |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
                [0, 1, 0.6],
                [2, 2, 0.8]
        ])

source = BatchOperator.fromDataframe(df, schemaStr='uid int, iid int, label double')

filepath = "/tmp/abc.xlsx"
tsvSink = XlsSinkBatchOp()\
    .setFilePath(filepath)\
    .setOverwriteSink(True)

source.link(tsvSink)

BatchOperator.execute()

tsvSource = XlsSourceBatchOp().setFilePath(filepath).setSchemaStr("f0 int, f1 int, f2 double")
tsvSource.print()

```
### Java 代码
```java
package javatest.com.alibaba.alink.batch.source;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.XlsSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.XlsSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TsvSourceBatchOpTest {
	@Test
	public void testTsvSourceBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, 1, 0.6),
			Row.of(2, 2, 0.8)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "uid int, iid int, label double");
		String filepath = "/tmp/abc.xlsx";
		BatchOperator <?> tsvSink = new XlsSinkBatchOp()
			.setFilePath(filepath)
			.setOverwriteSink(true);
		source.link(tsvSink);
		BatchOperator.execute();
		BatchOperator <?> tsvSource = new XlsSourceBatchOp().setFilePath(filepath).setSchemaStr("f0 int, f1 int, f2 double");
		tsvSource.print();
	}
}
```

