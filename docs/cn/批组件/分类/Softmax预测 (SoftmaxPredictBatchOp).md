# Softmax预测 (SoftmaxPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.SoftmaxPredictBatchOp

Python 类名：SoftmaxPredictBatchOp


## 功能介绍
Softmax算法是Logistic回归算法的推广，Logistic回归主要是用来处理二分类问题，而Softmax回归则是处理多分类问题。

### 算法原理
面对多分类问题，建立代价函数，然后通过优化方法迭代求解出最优的模型参数。具体原理可参考文献。

### 算法使用

该算法经常应用到多分类问题中，类似情感分析、手写字识别等问题都可以使用Softmax算法，该算法支持稀疏和稠密两种输入样本。

### 文献或出处
[1] Brown, Peter F., et al. "Class-based n-gram models of natural language." Computational linguistics 18.4 (1992): 467-480.

[2] Goodman, Joshua. "Classes for fast maximum entropy training." 2001 IEEE International Conference on Acoustics, Speech, and Signal Processing. Proceedings (Cat. No. 01CH37221). Vol. 1. IEEE, 2001.


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例 
### Python 代码
```python
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

public class SoftmaxPredictBatchOpTest {
	@Test
	public void testSoftmaxPredictBatchOp() throws Exception {
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



