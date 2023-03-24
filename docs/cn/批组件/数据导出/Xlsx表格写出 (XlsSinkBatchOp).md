# Xlsx表格写出 (XlsSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.XlsSinkBatchOp

Python 类名：XlsSinkBatchOp


## 功能介绍
写xlsx后缀名的表格文件，使用时需要下载xls插件。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |


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

