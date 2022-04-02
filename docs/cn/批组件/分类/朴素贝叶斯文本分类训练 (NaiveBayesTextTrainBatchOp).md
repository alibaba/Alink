# 朴素贝叶斯文本分类训练 (NaiveBayesTextTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp

Python 类名：NaiveBayesTextTrainBatchOp


## 功能介绍

训练一个朴素贝叶斯文本分类模型用于多分类任务。

### 算法原理

朴素贝叶斯算法基于贝叶斯定理和一个"朴素"的假设：各特征间两两条件独立。

通过贝叶斯定理可以在给定特征$(x_1,\cdots,x_n)$时计算类别为$y$的概率：$P(y|x_1,\cdots,x_n)=\frac{P(y)P(x_1,\cdots,x_n|y)}{P(x_1,\cdots,x_n)
}$，而通过特征间两两独立的假设可以将上面公式简化为：$P(y|x_1,\cdots,x_n)=\frac{P(y)\prod_{i = 1}^{n}P(x_i|y)}{P(x_1,\cdots,x_n)}$。

在朴素贝叶斯用于文本分类时，文本中的词语（token）对应一个特征。 类别$c$的概率可以估算为 $\hat P(c)=\frac{N_c}{N}$，其中 $N_c$ 是类别为$c$的总文本数，$N$的总文本数。
词语$t_i$在类别$c$所包含的文本出现的频次用 $T_{ct_i}$表示，那么可以用于估算概率 $\hat P(t|c)= \frac{T_{ct_i}}{\sum_{t'\in V}T_{ct'}}$。

与一般的朴素贝叶斯分类模型类似，可以添加平滑系数来解决$T_{ct_i}$ 为 0 时的问题。

上面描述的是多项分布时的模型，即$T_{ct_i}$ 可以去大于1的值。如果考虑的是二项分布，那么$T_{ct_i}$只能取值0或者1。

### 使用方式

该组件是训练组件，需要配合预测组件 NaiveBayesTextPredictBatch/StreamOp 使用。

为了训练朴素贝叶斯模型，需要指定参数向量列名（vectorCol）和标签列名（labelCol）。 通过参数模型类型可以设置使用多项分布或者二项分布。 平滑因子可以通过参数 smoothing 指定，默认为不平滑。

组件还支持设置每条样本的权重，通过参数权重列（weightCol）指定。

### 文献索引

Naive Bayes text
classification: [https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html](https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| smoothing | 算法参数 | 光滑因子，默认为1.0 | Double |  | 1.0 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| modelType | 模型类型 | 取值为 Multinomial 或 Bernoulli | String |  | "Multinomial" |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
          ["$31$0:1.0 1:1.0 2:1.0 30:1.0","1.0  1.0  1.0  1.0", '1'],
          ["$31$0:1.0 1:1.0 2:0.0 30:1.0","1.0  1.0  0.0  1.0", '1'],
          ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
          ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0']
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='sv string, dv string, label string')

# train op
ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextTrainBatchOpTest {
	@Test
	public void testNaiveBayesTextTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", "1"),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", "1"),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "1"),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "1"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0")
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data, "sv string, dv string, label string");
		BatchOperator <?> ns = new NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label");
		BatchOperator model = batchData.link(ns);
		BatchOperator <?> predictor = new NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols("sv",
			"label").setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```
### 运行结果

| sv                             | label | pred |
|--------------------------------|-------|------|
| "$31$0:1.0 1:1.0 2:1.0 30:1.0" | 1     | 1    |
| "$31$0:1.0 1:1.0 2:0.0 30:1.0" | 1     | 1    |
| "$31$0:1.0 1:0.0 2:1.0 30:1.0" | 1     | 1    |
| "$31$0:1.0 1:0.0 2:1.0 30:1.0" | 1     | 1    |
| "$31$0:0.0 1:1.0 2:1.0 30:0.0" | 0     | 0    |
| "$31$0:0.0 1:1.0 2:1.0 30:0.0" | 0     | 0    |
| "$31$0:0.0 1:1.0 2:1.0 30:0.0" | 0     | 0    |
