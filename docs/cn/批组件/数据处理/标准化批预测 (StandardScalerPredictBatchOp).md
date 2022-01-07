# 标准化批预测 (StandardScalerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.StandardScalerPredictBatchOp

Python 类名：StandardScalerPredictBatchOp


## 功能介绍

标准化是对数据进行按正态化处理的组件

使用标准化训练组件训练的模型，对数据做标准化处理

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]


inOp = BatchOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')
         

# train
trainOp = StandardScalerTrainBatchOp()\
           .setSelectedCols(selectedColNames)

trainOp.linkFrom(inOp)

# batch predict
predictOp = StandardScalerPredictBatchOp()
predictOp.linkFrom(trainOp, inOp).print()


```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StandardScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.StandardScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StandardScalerPredictBatchOpTest {
	@Test
	public void testStandardScalerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 10.0, 100),
			Row.of("b", -2.5, 9),
			Row.of("c", 100.2, 1),
			Row.of("d", -99.9, 100),
			Row.of("a", 1.4, 1),
			Row.of("b", -2.2, 9),
			Row.of("c", 100.9, 1)
		);

		String[] selectedColNames = new String[] {"col2", "col3"};
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "col1 string, col2 double, col3 int");
		BatchOperator <?> trainOp = new StandardScalerTrainBatchOp()
			.setSelectedCols(selectedColNames);
		trainOp.linkFrom(inOp);
		BatchOperator <?> predictOp = new StandardScalerPredictBatchOp();
		predictOp.linkFrom(trainOp, inOp).print();
	}
}
```
### 运行结果


col1|col2|col3
----|----|----
a|-0.0784|1.4596
b|-0.2592|-0.4814
c|1.2270|-0.6521
d|-1.6687|1.4596
a|-0.2028|-0.6521
b|-0.2549|-0.4814
c|1.2371|-0.6521



