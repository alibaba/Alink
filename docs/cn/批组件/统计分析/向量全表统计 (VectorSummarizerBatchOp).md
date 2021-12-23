# 向量全表统计 (VectorSummarizerBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp

Python 类名：VectorSummarizerBatchOp


## 功能介绍

针对vector数据，进行全表统计

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |



## 代码示例
### Python 代码
无python接口
 
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class VectorSummarizerBatchOpTest {

	@Test
	public void testVectorCorrelationBatchOp() throws Exception {
		Row[] testArray = new Row[] {
			Row.of(7, "0.0  0.0  18.0  1.0", 1.0),
			Row.of(8, "0.0  1.0  12.0  0.0", 0.0),
			Row.of(9, "1.0  0.0  15.0  0.1", 0.0),
		};

		String[] colNames = new String[] {"id", "features", "clicked"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorSummarizerBatchOp test = new VectorSummarizerBatchOp()
			.setSelectedCol("features");

		test.linkFrom(source);

		test.lazyPrintVectorSummary();

		BatchOperator.execute();
	}
}

```

### 运行结果

DenseVectorSummary:

| id|count|sum|  mean|variance|stdDev|min|max|normL1| normL2|
|---|-----|---|------|--------|------|---|---|------|-------|
|  0|    3|  1|0.3333|  0.3333|0.5774|  0|  1|     1|      1|
|  1|    3|  1|0.3333|  0.3333|0.5774|  0|  1|     1|      1|
|  2|    3| 45|    15|       9|     3| 12| 18|    45|26.3249|
|  3|    3|1.1|0.3667|  0.3033|0.5508|  0|  1|   1.1|  1.005|

