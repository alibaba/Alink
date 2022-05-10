# Table数据源 (TableSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.TableSourceStreamOp

Python 类名：TableSourceStreamOp


## 功能介绍
从Table中生成StreamOperator数据

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df_data = pd.DataFrame([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
inOp = StreamOperator.fromDataframe(df_data, schemaStr='id int, vec string')
TableSourceStreamOp(inOp.getOutputTable()).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TableSourceStreamOpTest {
	@Test
	public void testTableSourceStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "1 1 1"),
			Row.of(2, "2 2 2")
		);
		StreamOperator <?> inOp = new MemSourceStreamOp(df_data, "id int, vec string");
		new TableSourceStreamOp(inOp.getOutputTable()).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
id|vec
---|---
1|1 1 1
0|0 0 0
2|2 2 2

