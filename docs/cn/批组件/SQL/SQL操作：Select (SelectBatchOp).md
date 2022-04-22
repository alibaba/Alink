# SQL操作：Select (SelectBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.SelectBatchOp

Python 类名：SelectBatchOp


## 功能介绍
对批式数据进行sql的SELECT操作。

### 使用方式

SELECT 语句中支持的内置函数可以参考 Flink
对应版本的官方文档： [System (Built-in) Functions](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/functions/systemfunctions/)
。 但需要注意，有些内置函数在旧 planner
中不支持，在这里列出：[Unsupported Built-In Functions](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/legacy_planner/#unsupported-built-in-functions)
。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |

## 代码示例

### Python 代码

```python
df = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003, 3.2]
])

batch_data = BatchOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')
batch_data.link(SelectBatchOp().setClause("f1 as name")).print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import org.junit.Test;

public class SelectBatchOpTest {
	@Test
	public void testSelectBatchOp() throws Exception {
        List <Row> df = Arrays.asList(
    	    Row.of("Ohio", 2000, 1.5),
    		Row.of("Ohio", 2001, 1.7),
    		Row.of("Ohio", 2002, 3.6),
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2002, 2.9),
    		Row.of("Nevada", 2003, 3.2)
    	);
    	BatchOperator <?> data = new MemSourceBatchOp(df, "f1 string, f2 int, f3 double");
		data.link(new SelectBatchOp().setClause("f1 as name")).print();
	}
}
```

### 运行结果

name
----
Ohio
Ohio
Ohio
Nevada
Nevada
Nevada
