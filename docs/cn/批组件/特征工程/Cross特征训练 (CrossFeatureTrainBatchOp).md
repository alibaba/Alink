# Cross特征训练 (CrossFeatureTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.CrossFeatureTrainBatchOp

Python 类名：CrossFeatureTrainBatchOp


## 功能介绍

将选定的特征列组合成单个向量类型的特征。

### 使用方式

该组件是训练组件，需要配合预测组件 CrossFeaturePredictBatch/StreamOp 使用。

为了训练模型，需要指定参与组合的特征列名（selectedCols）。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
["1.0", "1.0", 1.0, 1],
["1.0", "1.0", 0.0, 1],
["1.0", "0.0", 1.0, 1],
["1.0", "0.0", 1.0, 1],
["2.0", "3.0", None, 0],
["2.0", "3.0", 1.0, 0],
["0.0", "1.0", 2.0, 0],
["0.0", "1.0", 1.0, 0]])
data = BatchOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
train = CrossFeatureTrainBatchOp().setSelectedCols(['f0','f1','f2']).linkFrom(data)
CrossFeaturePredictBatchOp().setOutputCol("cross").linkFrom(train, data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.CrossFeaturePredictBatchOp;
import com.alibaba.alink.operator.batch.feature.CrossFeatureTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CrossFeatureTrainBatchOpTest {
	@Test
	public void testCrossFeatureTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1.0", "1.0", 1.0, 1),
			Row.of("1.0", "1.0", 0.0, 1),
			Row.of("1.0", "0.0", 1.0, 1),
			Row.of("1.0", "0.0", 1.0, 1),
			Row.of("2.0", "3.0", null, 0),
			Row.of("2.0", "3.0", 1.0, 0),
			Row.of("0.0", "1.0", 2.0, 0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string, f1 string, f2 double, label int");
		BatchOperator <?> train = new CrossFeatureTrainBatchOp().setSelectedCols("f0", "f1", "f2").linkFrom(data);
		new CrossFeaturePredictBatchOp().setOutputCol("cross").linkFrom(train, data).print();
	}
}
```

### 运行结果

f0|f1|f2|label|cross
--|--|--|-----|-----
1.0|1.0|1.0000|1|$36$0:1.0
1.0|1.0|0.0000|1|$36$9:1.0
1.0|0.0|1.0000|1|$36$6:1.0
1.0|0.0|1.0000|1|$36$6:1.0
2.0|3.0|null|0|$36$22:1.0
2.0|3.0|1.0000|0|$36$4:1.0
0.0|1.0|2.0000|0|$36$29:1.0
0.0|1.0|1.0000|0|$36$2:1.0
