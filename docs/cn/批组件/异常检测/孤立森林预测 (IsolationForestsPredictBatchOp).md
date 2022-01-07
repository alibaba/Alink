# 孤立森林预测 (IsolationForestsPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.IsolationForestsPredictBatchOp

Python 类名：IsolationForestsPredictBatchOp


## 功能介绍

isolationforests是一种常用的树模型，在异常检测中常常可以取得很好的效果

[Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, 0],
    [2.0, 1],
    [3.0, 2],
    [4.0, 3]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr='f0 double, f1 double')

trainOp = IsolationForestsTrainBatchOp()\
    .setFeatureCols(['f0', 'f1'])\
    .linkFrom(batchSource)
predictBatchOp = IsolationForestsPredictBatchOp()\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IsolationForestsPredictBatchOp;
import com.alibaba.alink.operator.batch.outlier.IsolationForestsTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.outlier.IsolationForestsPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IsolationForestsPredictBatchOpTest {
	@Test
	public void testIsolationForestsPredictBatchOp() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of(1.0, 0.0),
			Row.of(2.0, 1.0),
			Row.of(3.0, 2.0),
			Row.of(4.0, 3.0)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(sourceFrame, "f0 double, f1 double");
		BatchOperator <?> trainOp =
			new IsolationForestsTrainBatchOp().setFeatureCols("f0", "f1").linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new IsolationForestsPredictBatchOp().setPredictionCol("pred");
		predictBatchOp
			.linkFrom(trainOp, batchSource)
			.print();
	}
}
```

### 运行结果

f0|f1|pred
---|---|----
1.0000|0.0000|0.3817
2.0000|1.0000|0.3817
3.0000|2.0000|0.3817
4.0000|3.0000|0.3817

