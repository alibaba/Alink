# KNN流式预测 (KnnPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.classification.KnnPredictStreamOp

Python 类名：KnnPredictStreamOp


## 功能介绍
KNN流式预测组件，读取模型和数据，数据流经过KNN计算逻辑输出预测结果。

KNN (K Nearest Neighbor）是一种分类算法。
KNN算法的核心思想是如果一个样本在特征空间中的k个最相邻的样本中的大多数属于某一个类别;
则该样本也属于这个类别，并具有这个类别上样本的特性。

相比于Huge版KNN，此KNN的优势在于训练数据（即KNN中的字典表）较小时，速度较快。
此外，KNN的训练与一般机器学习模型的训练过程不同：在KNN训练中我们只进行一些字典表的预处理，而在预测过程中才会进行计算预测每个数据点的类别。
因此，KNN的训练和预测通常同时使用，一般不单独使用。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| k | topK | topK | Integer |  | 10 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1, 0, 0],
    [2, 8, 8],
    [1, 1, 2],
    [2, 9, 10],
    [1, 3, 1],
    [2, 10, 7]
])

dataSourceOp = BatchOperator.fromDataframe(df, schemaStr="label int, f0 int, f1 int")
streamData = StreamOperator.fromDataframe(df, schemaStr="label int, f0 int, f1 int")

knnModel = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
predictor = KnnPredictStreamOp(knnModel).setPredictionCol("pred").setK(4)
predictor.linkFrom(streamData).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.KnnPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.KnnTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.KnnPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KnnPredictBatchOpTest {
	@Test
	public void testKnnPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 0, 0),
			Row.of(2, 8, 8),
			Row.of(1, 1, 2),
			Row.of(2, 9, 10),
			Row.of(1, 3, 1),
			Row.of(2, 10, 7)
		);
		BatchOperator <?> dataSourceOp = new MemSourceBatchOp(df, "label int, f0 int, f1 int");
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "label int, f0 int, f1 int");
		
		BatchOperator <?> trainOp = new KnnTrainBatchOp().setFeatureCols("f0", "f1").setLabelCol("label")
			.setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp);
		StreamOperator <?> predictOp = new KnnPredictStreamOp(trainOp).setPredictionCol("pred").setK(4).linkFrom(streamData);
		predictOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

label|f0|f1|pred
-----|---|---|----
1|0|0|1
2|8|8|2
1|1|2|1
2|9|10|2
1|3|1|1
2|10|7|2
