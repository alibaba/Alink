# 线性回归训练 (LinearRegTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp

Python 类名：LinearRegTrainBatchOp


## 功能介绍
线性回归算法是经典的回归算法，通过对带有回归值的样本集合训练得到回归模型，使用模型预测样本的回归值。线性回归组件支持稀疏、稠密两种数据格式，并且支持带权重样本训练。

### 算法原理
面对回归类问题，线性回归利用称为线性回归方程的最小平方函数对一个或多个自变量和因变量之间关系进行建模的一种回归分析。

### 算法使用
线性回归模型经常被用来做一些数值型变量的预测，类似房价预测、销售量预测、贷款额度预测、温度预测、适度预测等。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献或出处
[1] Seber, George AF, and Alan J. Lee. Linear regression analysis. John Wiley & Sons, 2012.

[2] https://baike.baidu.com/item/%E7%BA%BF%E6%80%A7%E5%9B%9E%E5%BD%92/8190345?fr=aladdin


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | [0.0, +inf) | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| l2 | L2 正则化系数 | L2 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| learningRate | 学习率 | 优化算法的学习率，默认0.1。 | Double |  |  | 0.1 |
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

lr = LinearRegTrainBatchOp()\
        .setFeatureCols(["f0","f1"])\
        .setLabelCol("label")

model = batchData.link(lr)

predictor = LinearRegPredictBatchOp()\
    .setPredictionCol("pred")

predictor.linkFrom(model, batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LinearRegTrainBatchOpTest {
	@Test
	public void testLinearRegTrainBatchOp() throws Exception {
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
		BatchOperator <?> lr = new LinearRegTrainBatchOp()
			.setFeatureCols("f0", "f1")
			.setLabelCol("label");
		BatchOperator model = batchData.link(lr);
		BatchOperator <?> predictor = new LinearRegPredictBatchOp()
			.setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```

### 运行结果
f0 | f1 | label | pred
---|----|-------|-----
   2 |  1   |   1  | 1.000014
   3 |  2   |   1  | 1.538474
   4 |  3   |   2  | 2.076934
   2 |  4   |   1  | 1.138446
   2 |  2   |   1  | 1.046158
   4 |  3   |   2  | 2.076934
   1 |  2   |   1  | 0.553842
   5 |  3   |   3  | 2.569250

