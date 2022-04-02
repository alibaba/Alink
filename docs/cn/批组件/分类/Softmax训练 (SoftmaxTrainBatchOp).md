# Softmax训练 (SoftmaxTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.SoftmaxTrainBatchOp

Python 类名：SoftmaxTrainBatchOp


## 功能介绍
Softmax算法是Logistic回归算法的推广，Logistic回归主要是用来处理二分类问题，而Softmax回归则是处理多分类问题。

### 算法原理
面对多分类问题，建立代价函数，然后通过优化方法迭代求解出最优的模型参数。具体原理可参考文献。

### 算法使用

该算法经常应用到多分类问题中，类似情感分析、手写字识别等问题都可以使用Softmax算法，该算法支持稀疏和稠密两种输入样本。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献或出处
[1] Brown, Peter F., et al. "Class-based n-gram models of natural language." Computational linguistics 18.4 (1992): 467-480.

[2] Goodman, Joshua. "Classes for fast maximum entropy training." 2001 IEEE International Conference on Acoustics, Speech, and Signal Processing. Proceedings (Cat. No. 01CH37221). Vol. 1. IEEE, 2001.


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  | true |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | null |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
       [2, 1, 1],
       [3, 2, 1],
       [4, 3, 2],
       [2, 4, 1],
       [2, 2, 1],
       [4, 3, 2],
       [1, 2, 1],
       [5, 3, 3]
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
dataTest = batchData

colnames = ["f0","f1"]
lr = SoftmaxTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = SoftmaxPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.SoftmaxPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.SoftmaxTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SoftmaxTrainBatchOpTest {
	@Test
	public void testSoftmaxTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 3)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data, "f0 int, f1 int, label int");
		BatchOperator dataTest = batchData;
		BatchOperator <?> lr = new SoftmaxTrainBatchOp().setFeatureCols("f0", "f1").setLabelCol("label");
		BatchOperator model = batchData.link(lr);
		BatchOperator <?> predictor = new SoftmaxPredictBatchOp().setPredictionCol("pred");
		predictor.linkFrom(model, dataTest).print();
	}
}
```
### 运行结果
f0 | f1 | label | pred 
---|----|-------|-----
2|1|1|1
3|2|1|1
4|3|2|2
2|4|1|1
2|2|1|1
4|3|2|2
1|2|1|1
5|3|3|3


## 备注

1. 该组件的输入为训练数据，输出为Softmax模型。
2. 参数数据库的使用方式可以覆盖多个参数的使用方式。

