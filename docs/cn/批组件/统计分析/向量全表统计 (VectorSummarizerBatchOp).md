# 向量全表统计 (VectorSummarizerBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp

Python 类名：VectorSummarizerBatchOp


## 功能介绍

* 全表统计用来计算整表的统计量, 包含count(个数),numValidValue(有效值个数), numMissingValue(缺失值个数), sum(求和), mean(均值), standardDeviation(标准差), variance(方差), min(最小值), max(最大值), normL1(L1范数), normL2(L2范数)。

* 只支持vector列

* 只支持java接口

### 使用方式
* 打印统计结果.

```java
test.lazyPrintVectorSummary();
```

* 获取相应的统计值

```java
    summary = summarizer.linkFrom(source).collectVectorSummary()
    print(summary.sum('f_double'))
    print(summary.mean('f_double'))
    print(summary.variance('f_double'))
    print(summary.standardDeviation('f_double'))
    print(summary.min('f_double'))
    print(summary.max('f_double'))
    print(summary.normL1('f_double'))
    print(summary.normL2('f_double'))
    print(summary.numValidValue('f_double'))
    print(summary.numMissingValue('f_double'))
```

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

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

