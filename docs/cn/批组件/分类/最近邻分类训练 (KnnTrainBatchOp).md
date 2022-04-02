# 最近邻分类训练 (KnnTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.KnnTrainBatchOp

Python 类名：KnnTrainBatchOp


## 功能介绍

KNN (K Nearest Neighbor）是一种分类算法。
KNN算法的核心思想是如果一个样本在特征空间中的k个最相邻的样本中的大多数属于某一个类别;
则该样本也属于这个类别，并具有这个类别上样本的特性。

KNN的训练与一般机器学习模型的训练过程不同：在KNN训练中我们只进行一些字典表的预处理，而在预测过程中才会进行计算预测每个数据点的类别。
因此，KNN的训练和预测通常同时使用，一般不单独使用。

支持 EUCLIDEAN 和 COSINE 两种距离计算方式。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |


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

trainOp = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
predictOp = KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp)
predictOp.print()

trainOp = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
predictOp = KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp)
predictOp.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.KnnPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.KnnTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KnnTrainBatchOpTest {
	@Test
	public void testKnnTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 0, 0),
			Row.of(2, 8, 8),
			Row.of(1, 1, 2),
			Row.of(2, 9, 10),
			Row.of(1, 3, 1),
			Row.of(2, 10, 7)
		);
		BatchOperator <?> dataSourceOp = new MemSourceBatchOp(df, "label int, f0 int, f1 int");
		BatchOperator <?> trainOp = new KnnTrainBatchOp().setFeatureCols("f0", "f1").setLabelCol("label")
			.setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp);
		BatchOperator <?> predictOp = new KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp,
			dataSourceOp);
		predictOp.print();
		trainOp = new KnnTrainBatchOp().setFeatureCols("f0", "f1").setLabelCol("label").setDistanceType("EUCLIDEAN")
			.linkFrom(dataSourceOp);
		predictOp = new KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp);
		predictOp.print();
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
