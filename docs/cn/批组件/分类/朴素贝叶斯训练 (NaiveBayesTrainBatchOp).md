# 朴素贝叶斯训练 (NaiveBayesTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp

Python 类名：NaiveBayesTrainBatchOp


## 功能介绍

训练一个朴素贝叶斯模型用于多分类任务。

### 算法原理

朴素贝叶斯算法基于贝叶斯定理和一个"朴素"的假设：各特征间两两条件独立。

通过贝叶斯定理可以在给定特征$(x_1,\cdots,x_n)$时计算类别为$y$的概率：$P(y|x_1,\cdots,x_n)=\frac{P(y)P(x_1,\cdots,x_n|y)}{P(x_1,\cdots,x_n)
}$，而通过特征间两两独立的假设可以将上面公式简化为：$P(y|x_1,\cdots,x_n)=\frac{P(y)\prod_{i = 1}^{n}P(x_i|y)}{P(x_1,\cdots,x_n)}$。

对于连续型特征$x_i$，通常假设 $P(x_i|y)$满足高斯分布 $(\mu_y,\sigma_y)$，参数可以通过对训练数据进行最大似然估计得到。 对于离散型特征$x_i$，$P(x_i|y)=\frac{N_{yi} +
\alpha}{N_{y}+\alpha n}$，其中 $N_{yi}$ 表示类别为$y$特征$x_i$共同出现的样本数， $N_y$表示类别 $y$ 的样本数，$\alpha$ 是平滑系数。

### 使用方式

该组件是训练组件，需要配合预测组件 NaiveBayesPredictBatch/StreamOp 使用。

为了训练朴素贝叶斯模型，需要指定参数特征列名（featureCols）和标签列名（labelCol）。
特征列名中，数值类型的列默认看作连续型特征处理，如果需要强制作为离散型特征处理，需要将这些列的列名添加到参数离散特征列名（categoricalCol）中。 平滑因子可以通过参数 smoothing 指定，默认为不平滑。

组件还支持设置每条样本的权重，通过参数权重列（weightCol）指定。

### 文献索引

H. Zhang (2004). [The optimality of Naive Bayes.](https://www.cs.unb.ca/~hzhang/publications/FLAIRS04ZhangH.pdf) Proc.
FLAIRS.

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  | 所选列类型为 [BIGINTEGER, BOOLEAN, INTEGER, LONG, STRING] |  |
| smoothing | 算法参数 | 光滑因子，默认为0.0 | Double |  | [0.0, +inf) | 0.0 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [DOUBLE] | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
       [1.0, 1.0, 0.0, 1.0, 1],
       [1.0, 0.0, 1.0, 1.0, 1],
       [1.0, 0.0, 1.0, 1.0, 1],
       [0.0, 1.0, 1.0, 0.0, 0],
       [0.0, 1.0, 1.0, 0.0, 0],
       [0.0, 1.0, 1.0, 0.0, 0],
       [0.0, 1.0, 1.0, 0.0, 0],
       [1.0, 1.0, 1.0, 1.0, 1],
       [0.0, 1.0, 1.0, 0.0, 0]
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 double, f1 double, f2 double, f3 double, label int')

# train op
colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTrainBatchOpTest {
	@Test
	public void testNaiveBayesTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1.0, 1.0, 0.0, 1.0, 1),
			Row.of(1.0, 0.0, 1.0, 1.0, 1),
			Row.of(1.0, 0.0, 1.0, 1.0, 1),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(1.0, 1.0, 1.0, 1.0, 1),
			Row.of(0.0, 1.0, 1.0, 0.0, 0)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data,
			"f0 double, f1 double, f2 double, f3 double, label int");
		BatchOperator <?> ns = new NaiveBayesTrainBatchOp().setFeatureCols("f0", "f1", "f2", "f3").setLabelCol(
			"label");
		BatchOperator model = batchData.link(ns);
		BatchOperator <?> predictor = new NaiveBayesPredictBatchOp().setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```
### 运行结果

| f0  | f1  | f2  | f3  | label | pred |
|-----|-----|-----|-----|-------|------|
| 1.0 | 1.0 | 0.0 | 1.0 | 1     | 1    |
| 1.0 | 0.0 | 1.0 | 1.0 | 1     | 1    |
| 1.0 | 0.0 | 1.0 | 1.0 | 1     | 1    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 1.0 | 1.0 | 1.0 | 1.0 | 1     | 1    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
