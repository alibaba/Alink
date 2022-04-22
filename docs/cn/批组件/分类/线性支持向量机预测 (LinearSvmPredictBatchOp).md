# 线性支持向量机预测 (LinearSvmPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.LinearSvmPredictBatchOp

Python 类名：LinearSvmPredictBatchOp


## 功能介绍
线性SVM算法是经典的二分类算法，通过对打标签样本集合训练得到模型，使用模型预测样本的标签。逻辑回归组件支持稀疏、稠密两种数据格式。

### 算法原理
SVM使用铰链损失函数（hinge loss）计算经验风险（empirical risk）并在求解系统中加入了正则化项以优化结构风险，是一个具有稀疏性
和稳健性的分类器。

### 算法使用
SVM在各领域的模式识别问题中有应用，包括人像识别、文本分类、手写字符识别、生物信息学等。

### 文献
[1] Vapnik, V．Statistical learning theory. 1998 (Vol. 3). ．New York, NY：Wiley，1998：Chapter 10-11, pp.401-492.

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

df_data = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 2]
])

input = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')

dataTest = input
colnames = ["f0","f1"]
svm = LinearSvmTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = input.link(svm)

predictor = LinearSvmPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LinearSvmPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.LinearSvmTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LinearSvmPredictBatchOpTest {
	@Test
	public void testLinearSvmPredictBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 2)
		);
		BatchOperator <?> input = new MemSourceBatchOp(df_data, "f0 int, f1 int, label int");
		BatchOperator dataTest = input;
		BatchOperator <?> svm = new LinearSvmTrainBatchOp().setFeatureCols("f0", "f1").setLabelCol("label");
		BatchOperator model = input.link(svm);
		BatchOperator <?> predictor = new LinearSvmPredictBatchOp().setPredictionCol("pred");
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
5|3|2|2



