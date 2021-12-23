# SOS异常检测 (SosBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.SosBatchOp

Python 类名：SosBatchOp


## 功能介绍
SOS (Stochastic Outlier Selection）是一种affinity based离群点检测算法。
它通常用于过滤掉噪音样本，从而使得机器学习的模型更准确。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| perplexity | 邻近因子 | 邻近因子。它的近似含义是当某个点的近邻个数小于"邻近因子"个时，这个点的离群score会比较高。 | Double |  | 4.0 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
  ["0.0,0.0"],
  ["0.0,1.0"],
  ["1.0,0.0"],
  ["1.0,1.0"],
  ["5.0,5.0"],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='features string')
sos = SosBatchOp().setVectorCol("features").setPredictionCol("outlier_score").setPerplexity(3.0)

output = sos.linkFrom(data)
output.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.SosBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SosBatchOpTest {
	@Test
	public void testSosBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("0.0,0.0"),
			Row.of("0.0,1.0"),
			Row.of("1.0,0.0"),
			Row.of("1.0,1.0"),
			Row.of("5.0,5.0")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "features string");
		BatchOperator <?> sos = new SosBatchOp().setVectorCol("features").setPredictionCol("outlier_score")
			.setPerplexity(3.0);
		BatchOperator <?> output = sos.linkFrom(data);
		output.print();
	}
}
```

### 运行结果

features|outlier_score
--------|-------------
1.0,1.0|0.12396819612216292
0.0,0.0|0.27815186043725715
0.0,1.0|0.24136320497783578
1.0,0.0|0.24136320497783578
5.0,5.0|0.9998106220648153
