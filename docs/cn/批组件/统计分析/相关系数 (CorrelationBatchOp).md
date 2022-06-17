# 相关系数 (CorrelationBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp

Python 类名：CorrelationBatchOp


## 功能介绍

* 相关系数算法用于计算一个矩阵中每一列之间的相关系数，范围在[-1,1]之间。计算的时候，count数按两列间同时非空的元素个数计算，两两列之间可能不同。

* 支持Pearson和Spearman两种相关系数

* 只支持数字类型列。如果想计算vector列，请参考VectorCorrelationBatchOp

### 使用方式
* 通过collectCorrelation()获取CorrelationMetric.
```python
corr = corrOp.collectCorrelation()
print(corr)
print(corr.getCorrelation())
print(corr.getCorrelation()[1][1])
```

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| method | 方法 | 方法：包含"PEARSON"和"SPEARMAN"两种，PEARSON。 | String |  | "PEARSON", "SPEARMAN" | "PEARSON" |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
         [0.0,0.0,0.0],
         [0.1,0.2,0.1],
         [0.2,0.2,0.8],
         [9.0,9.5,9.7],
         [9.1,9.1,9.6],
         [9.2,9.3,9.9]])

source = BatchOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')


corr = CorrelationBatchOp()\
            .setSelectedCols(["x1","x2","x3"])

corr = source.link(corr).collectCorrelation()
print(corr)

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CorrelationBatchOpTest {
	@Test
	public void testCorrelationBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0.0, 0.0, 0.0),
			Row.of(0.1, 0.2, 0.1),
			Row.of(0.2, 0.2, 0.8),
			Row.of(9.0, 9.5, 9.7),
			Row.of(9.1, 9.1, 9.6)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "x1 double, x2 double, x3 double");
		CorrelationBatchOp corr = new CorrelationBatchOp()
			.setSelectedCols("x1", "x2", "x3");
		CorrelationResult correlationResult = source.link(corr).collectCorrelation();
		System.out.println(correlationResult);
	}
}
```
### 运行结果

colName|x1|x2|x3
-------|---|---|---
x1|1.0000|0.9994|0.9990
x2|0.9994|1.0000|0.9986
x3|0.9990|0.9986|1.0000


