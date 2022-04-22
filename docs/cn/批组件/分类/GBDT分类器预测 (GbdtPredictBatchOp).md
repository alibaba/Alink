# GBDT分类器预测 (GbdtPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.GbdtPredictBatchOp

Python 类名：GbdtPredictBatchOp


## 功能介绍

## 功能介绍

梯度提升决策树(Gradient Boosting Decision Trees)二分类，是经典的基于梯度提升的有监督学习模型，可以用来解决二分类问题

### 算法原理

梯度提升决策树模型构建了一个由多棵决策树组成的组合模型。每一棵决策树对应一个弱学习器，将这些弱学习器组合在一起，可以达到比较好的分类或回归效果。

梯度提升的基本递推结构为：

$$F_{m}(x) = F_{m-1}(x) + \beta_{m}h(x;a_m)$$

其中 $h(x;a_m)$ 通常为一棵 CART[2] 决策树，${a_m}$ 为在这棵决策树下的分割变量，$\beta_{m}h(x;a_m)$ 为在 $h(x;a_m)$ 约束下的步长，通过这个递推结构即可得出最终模型。

### 算法使用

对于一些常见的二分类问题，都可以使用这个算法解决，模型拥有较好的性能，且拥有不错的可解释性，在实际场景中，应用较为广泛。

- 支持连续特征和离散特征
- 支持数据采样和特征采样
- 目标分类必须为两个

### 文献或出处

1. Friedman J H. Greedy function approximation: a gradient boosting machine[J]. Annals of statistics, 2001: 1189-1232.
2. Breiman, L., Friedman, J. H., Olshen, R. and Stone, C. (1983). Classification and Regression
Trees. Wadsworth, Belmont, CA.
3. [weka](https://www.cs.waikato.ac.nz/ml/weka/)
4. [xgboost](https://xgboost.readthedocs.io/en/stable/)
5. [lightgbm](https://lightgbm.readthedocs.io/en/latest/)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])
batchSource = BatchOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')

trainOp = GbdtTrainBatchOp()\
    .setLearningRate(1.0)\
    .setNumTrees(3)\
    .setMinSamplesPerLeaf(1)\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .linkFrom(batchSource)
predictBatchOp = GbdtPredictBatchOp()\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')
predictStreamOp = GbdtPredictStreamOp(trainOp)\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.GbdtPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.GbdtPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GbdtPredictBatchOpTest {
	@Test
	public void testGbdtPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);

		BatchOperator <?> batchSource = new MemSourceBatchOp(
			df, " f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator <?> streamSource = new MemSourceStreamOp(
			df, " f0 double, f1 string, f2 int, f3 int, label int");
		BatchOperator <?> trainOp = new GbdtTrainBatchOp()
			.setLearningRate(1.0)
			.setNumTrees(3)
			.setMinSamplesPerLeaf(1)
			.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new GbdtPredictBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");
		StreamOperator <?> predictStreamOp = new GbdtPredictStreamOp(trainOp)
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

f0|f1|f2|f3|label|pred|pred_detail
---|---|---|---|-----|----|-----------
1.0000|A|0|0|0|0|{"0":0.9849144946061075,"1":0.01508550539389248}
2.0000|B|1|1|0|0|{"0":0.9849144946061075,"1":0.01508550539389248}
3.0000|C|2|2|1|1|{"0":0.015085505393892529,"1":0.9849144946061075}
4.0000|D|3|3|1|1|{"0":0.015085505393892529,"1":0.9849144946061075}
