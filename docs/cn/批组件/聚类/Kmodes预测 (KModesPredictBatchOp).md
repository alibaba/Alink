# Kmodes预测 (KModesPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.KModesPredictBatchOp

Python 类名：KModesPredictBatchOp


## 功能介绍

KModes是一种用于离散数据/分类数据(categorical data)的聚类算法。 基本思想是：把n个对象分为k个簇，使簇内具有较小的的相异度(或者称距离)。 距离计算方法：两个字符串比较，相同为0，不同为1。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["pc", "Hp.com"],
    ["camera", "Hp.com"],
    ["digital camera", "Hp.com"],
    ["camera", "BestBuy.com"],
    ["digital camera", "BestBuy.com"],
    ["tv", "BestBuy.com"],
    ["flower", "Teleflora.com"],
    ["flower", "Orchids.com"]
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 string, f1 string')

kmodes = KModesTrainBatchOp()\
    .setFeatureCols(["f0", "f1"])\
    .setK(2)\
    .linkFrom(inOp1)
    
predict = KModesPredictBatchOp()\
    .setPredictionCol("pred")\
    .linkFrom(kmodes, inOp1)
    
kmodes.lazyPrint(10)
predict.print()

predict = KModesPredictStreamOp(kmodes)\
    .setPredictionCol("pred")\
    .linkFrom(inOp2)
    
predict.print()

StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.KModesPredictStreamOp;
import com.alibaba.alink.operator.batch.clustering.KModesPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KModesTrainBatchOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KModesPredictBatchOpTest {

	@Test
	public void testKModesPredictBatchOp() throws Exception {
		List <Row> dataPoints = Arrays.asList(
			Row.of("pc", "Hp.com"),
			Row.of("camera", "Hp.com"),
			Row.of("digital camera", "Hp.com"),
			Row.of("camera", "BestBuy.com"),
			Row.of("digital camera", "BestBuy.com"),
			Row.of("tv", "BestBuy.com"),
			Row.of("flower", "Teleflora.com"),
			Row.of("flower", "Orchids.com")
		);

		MemSourceBatchOp inOp1 = new MemSourceBatchOp(dataPoints, "f0 string, f1 string");
		MemSourceStreamOp inOp2 = new MemSourceStreamOp(dataPoints, "f0 string, f1 string");

		KModesTrainBatchOp kmodes = new KModesTrainBatchOp()
			.setFeatureCols(new String[]{"f0", "f1"})
			.setK(2)
			.linkFrom(inOp1);
		KModesPredictBatchOp kModesPredictBatchOp = new KModesPredictBatchOp()
			.setPredictionCol("pred")
			.linkFrom(kmodes, inOp1);

		kmodes.lazyPrint(10);
		kModesPredictBatchOp.print();

		KModesPredictStreamOp kModesPredictStreamOp = new KModesPredictStreamOp(kmodes)
			.setPredictionCol("pred")
			.linkFrom(inOp2);
		kModesPredictStreamOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f0|f1|pred
---|---|----
pc|Hp.com|1
flower|Teleflora.com|0
digital camera|BestBuy.com|1
digital camera|Hp.com|1
flower|Orchids.com|0
tv|BestBuy.com|0
camera|BestBuy.com|0
camera|Hp.com|1

