# Table数据读入 (TableSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.TableSourceBatchOp

Python 类名：TableSourceBatchOp


## 功能介绍
从Table中生成BatchOperator数据

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp.getOutputTable()
TableSourceBatchOp(inOp.getOutputTable()).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TableSourceBatchOpTest {
	@Test
	public void testTableSourceBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "1 1 1"),
			Row.of(2, "2 2 2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		inOp.getOutputTable();
		new TableSourceBatchOp(inOp.getOutputTable()).print();
	}
}
```
### 运行结果

id|vec
---|---
0|0 0 0
1|1 1 1
2|2 2 2

