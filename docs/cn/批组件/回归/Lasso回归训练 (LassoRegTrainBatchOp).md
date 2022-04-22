# Lasso回归训练 (LassoRegTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp

Python 类名：LassoRegTrainBatchOp


## 功能介绍
Lasso回归算法是由1996年Robert Tibshirani首次提出。是一种经典的回归算法。Lasso回归组件支持稀疏、稠密两种数据格式，并且支持带权重样本训练。

### 算法原理
Lasso回归算法通过构造一个惩罚函数得到一个较为精炼的模型，使得它压缩一些回归系数，即强制系数绝对值之和小于某个固定值；同时设定一些回归系数为零。因此保留了子集收缩的优点，是一种处理具有复共线性数据的有偏估计。

### 算法使用
Lasso回归模型应用领域和线性回归类似，经常被用来做一些数值型变量的预测，类似房价预测、销售量预测、贷款额度预测、温度预测、适度预测等。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献或出处
[1] Tibshirani, Robert. "Regression shrinkage and selection via the lasso." Journal of the Royal Statistical Society: Series B (Methodological) 58.1 (1996): 267-288.

[2] https://baike.baidu.com/item/LASSO/20366865?fr=aladdin


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| lambda | 希腊字母：lambda | 惩罚因子，必选 | Double | ✓ |  |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | [0.0, +inf) | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | "LBFGS", "GD", "Newton", "SGD", "OWLQN" | null |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  |  | true |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  |  | true |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]
])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')

lasso = LassoRegTrainBatchOp()\
        .setLambda(0.1)\
        .setFeatureCols(["f0","f1"])\
        .setLabelCol("label")

model = batchData.link(lasso)

predictor = LassoRegPredictBatchOp()\
        .setPredictionCol("pred")

predictor.linkFrom(model, batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LassoRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LassoRegTrainBatchOpTest {
	@Test
	public void testLassoRegTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 3)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int, label int");
		BatchOperator <?> lasso = new LassoRegTrainBatchOp()
			.setLambda(0.1)
			.setFeatureCols("f0", "f1")
			.setLabelCol("label");
		BatchOperator model = batchData.link(lasso);
		BatchOperator <?> predictor = new LassoRegPredictBatchOp()
			.setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```
### 运行结果
f0 | f1 | label | pred
---|----|-------|-----
 2 |  1     | 1 | 0.830304
   3 |  2    |  1 | 1.377312
   4 |  3    |  2 | 1.924320
   2 |  4    |  1 | 1.159119
   2 |  2    |  1 | 0.939909
   4 |  3    |  2 | 1.924320
   1 |  2    |  1 | 0.502506
   5 |  3    |  3 | 2.361724
