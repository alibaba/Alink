# 向量相关系数 (VectorCorrelationBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.VectorCorrelationBatchOp

Python 类名：VectorCorrelationBatchOp


## 功能介绍

针对vector数据，计算相关系数

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| method | 方法 | 方法：包含"PEARSON"和"SPEARMAN"两种，PEARSON。 | String |  | "PEARSON", "SPEARMAN" | "PEARSON" |




## 代码示例
### Python 代码
无python接口
 
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorCorrelationBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class VectorCorrelationBatchOpTest {

	@Test
	public void testVectorCorrelationBatchOp() throws Exception {
		Row[] testArray = new Row[] {
			Row.of(7, "0.0  0.0  18.0  1.0", 1.0),
			Row.of(8, "0.0  1.0  12.0  0.0", 0.0),
			Row.of(9, "1.0  0.0  15.0  0.1", 0.0),
		};

		String[] colNames = new String[] {"id", "features", "clicked"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorCorrelationBatchOp test = new VectorCorrelationBatchOp()
			.setSelectedCol("features");

		test.linkFrom(source);

		test.lazyPrintCorrelation();

		BatchOperator.execute();
	}
}

```

### 运行结果

Correlation:

|colName|      0|      1|     2|      3|
|-------|-------|-------|------|-------|
|      0|      1|   -0.5|     0|-0.4193|
|      1|   -0.5|      1|-0.866|-0.5766|
|      2|      0| -0.866|     1| 0.9078|
|      3|-0.4193|-0.5766|0.9078|      1|


