# 向量卡方选择器 (VectorChiSqSelectorBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.VectorChiSqSelectorBatchOp

Python 类名：VectorChiSqSelectorBatchOp


## 功能介绍

针对vector数据，进行特征筛选

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| fdr | 发现阈值 | 发现阈值, 默认值0.05 | Double |  |  | 0.05 |
| fpr | p value的阈值 | p value的阈值，默认值0.05 | Double |  |  | 0.05 |
| fwe | 错误率阈值 | 错误率阈值, 默认值0.05 | Double |  |  | 0.05 |
| numTopFeatures | 最大的p-value列个数 | 最大的p-value列个数, 默认值50 | Integer |  |  | 50 |
| percentile | 筛选的百分比 | 筛选的百分比，默认值0.1 | Double |  |  | 0.1 |
| selectorType | 筛选类型 | 筛选类型，包含"NumTopFeatures","percentile", "fpr", "fdr", "fwe"五种。 | String |  | "NumTopFeatures", "PERCENTILE", "FPR", "FDR", "FWE" | "NumTopFeatures" |



## 代码示例
### Python 代码
无python接口
 
### Java 代码
```java
package javatest.com.alibaba.alink.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.VectorChiSqSelectorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class VectorChiSqSelectorBatchOpTest {

	@Test
	public void testVectorChiSqSelectorBatchOp() throws Exception {
		Row[] testArray = new Row[] {
			Row.of(7, "0.0  0.0  18.0  1.0", 1.0),
			Row.of(8, "0.0  1.0  12.0  0.0", 0.0),
			Row.of(9, "1.0  0.0  15.0  0.1", 0.0),
		};

		String[] colNames = new String[] {"id", "features", "clicked"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorChiSqSelectorBatchOp test = new VectorChiSqSelectorBatchOp()
			.setSelectedCol("features")
			.setLabelCol("clicked");

		test.linkFrom(source);

		test.lazyPrintModelInfo();

		BatchOperator.execute();
	}

}


```

### 运行结果

```python
------------------------- ChisqSelectorModelInfo -------------------------
Number of Selector Features: 4
Number of Features: 4
Type of Selector: NumTopFeatures
Number of Top Features: 50

Selector Indices: 

    |VectorIndex|ChiSquare|PValue| DF|Selected|
    |-----------|---------|------|---|--------|
    |          3|        3|0.2231|  2|    true|
    |          2|        3|0.2231|  2|    true|
    |          0|     0.75|0.3865|  1|    true|
    |          1|     0.75|0.3865|  1|    true|
```


