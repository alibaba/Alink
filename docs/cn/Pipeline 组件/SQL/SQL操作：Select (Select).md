# SQL操作：Select (Select)
Java 类名：com.alibaba.alink.pipeline.sql.Select

Python 类名：Select


## 功能介绍

提供 SQL 的 SELECT 语句功能。

### 使用方式

该组件提供与 SelectBatch/StreamOp 相近的功能。

该组件既可以单独使用（直接调用 `transform`方法），也可以置于构建 `Pipeline` 中使用。 前一种场景的使用方式跟 SelectBatch/StreamOp 一致。

后一种场景时，有两点需要特别注意：

1. 语句需要严格输入 1 行输出 1 行，即满足 `map` 的功能；
2. 因为内部实现实际采用的是 Calcite 而非 Flink，因此可能不支持某些 Flink
   内置方法；经过测试可以使用的内置方法可以见[测试代码](https://github.com/alibaba/Alink/blob/master/core/src/test/java/com/alibaba/alink/operator/common/sql/SelectMapperTest.java)
   。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |

## 代码示例

### Python 代码

```python
URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
select = Select().setClause("category as label")
select.transform(data).print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.sql.Select;
import org.junit.Test;

public class SelectTest {
	@Test
	public void testSelect() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		Select select = new Select().setClause("category as label");
		select.transform(data).print();
	}
}
```

### 运行结果

| label           |
|-----------------|
| Iris-versicolor |
| Iris-setosa     |
| Iris-setosa     |
| Iris-setosa     |
| Iris-virginica  |
| ...             |
| Iris-setosa     |
| Iris-versicolor |
| Iris-virginica  |
| Iris-setosa     |
| Iris-versicolor |
