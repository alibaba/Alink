# 最近邻分类 (KnnClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.KnnClassifier

Python 类名：KnnClassifier


## 功能介绍
KNN (K Nearest Neighbor）是一种分类算法。
KNN算法的核心思想是如果一个样本在特征空间中的k个最相邻的样本中的大多数属于某一个类别;
则该样本也属于这个类别，并具有这个类别上样本的特性。

KNN的训练与一般机器学习模型的训练过程不同：在KNN训练中我们只进行一些字典表的预处理，而在预测过程中才会进行计算预测每个数据点的类别。
因此，KNN的训练和预测通常同时使用，一般不单独使用。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |
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
  [1, "0,0,0"],
  [1, "0.1,0.1,0.1"],
  [1, "0.2,0.2,0.2"],
  [0, "9,9,9"],
  [0, "9.1,9.1,9.1"],
  [0, "9.2,9.2,9.2"]
])

dataSource = BatchOperator.fromDataframe(df, schemaStr="label int, vec string")
knn = KnnClassifier().setVectorCol("vec") \
    .setPredictionCol("pred") \
    .setLabelCol("label") \
    .setK(3)

model = knn.fit(dataSource)
model.transform(dataSource).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.classification.KnnClassifier;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KnnClassifierTest {
	@Test
	public void testKnnClassifier() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, "0,0,0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(1, "0.2,0.2,0.2"),
			Row.of(0, "9,9,9"),
			Row.of(0, "9.1,9.1,9.1"),
			Row.of(0, "9.2,9.2,9.2")
		);
		BatchOperator <?> dataSource = new MemSourceBatchOp(df, "label int, vec string");
		KnnClassifier knn = new KnnClassifier().setVectorCol("vec")
			.setPredictionCol("pred")
			.setLabelCol("label")
			.setK(3);
		knn.fit(dataSource)
			.transform(dataSource)
			.print();
	}
}
```


### 运行结果

label|vec|pred
-----|---|----
1|0,0,0|1
1|0.1,0.1,0.1|1
1|0.2,0.2,0.2|1
0|9,9,9|0
0|9.1,9.1,9.1|0
0|9.2,9.2,9.2|0
