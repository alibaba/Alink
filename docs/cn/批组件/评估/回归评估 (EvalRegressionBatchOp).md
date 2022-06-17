# 回归评估 (EvalRegressionBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp

Python 类名：EvalRegressionBatchOp


## 功能介绍
对回归算法的预测结果进行效果评估，支持下列评估指标。

### 算法原理

对于每条样本$i$，$y_i$表示样本的真实标签，$f_i$表示回归模型预测的值。样本总数记为$N$。
支持计算下面的评估指标：

#### SST 总平方和(Sum of Squared for Total)

$SST=\sum_{i=1}^{N}(y_i-\bar{y})^2$

#### SSE 误差平方和(Sum of Squares for Error)

$SSE=\sum_{i=1}^{N}(y_i-f_i)^2$

#### SSR 回归平方和(Sum of Squares for Regression)

$SSR=\sum_{i=1}^{N}(f_i-\bar{y})^2$

#### R^2 判定系数(Coefficient of Determination)

$R^2=1-\dfrac{SSE}{SST}$

#### R 多重相关系数(Multiple Correlation Coeffient)

$R=\sqrt{R^2}$

#### MSE 均方误差(Mean Squared Error)

$MSE=\dfrac{1}{N}\sum_{i=1}^{N}(f_i-y_i)^2$

#### RMSE 均方根误差(Root Mean Squared Error)

$RMSE=\sqrt{MSE}$

#### SAE/SAD 绝对误差(Sum of Absolute Error/Difference)

$SAE=\sum_{i=1}^{N}|f_i-y_i|$

#### MAE/MAD 平均绝对误差(Mean Absolute Error/Difference)

$MAE=\dfrac{1}{N}\sum_{i=1}^{N}|f_i-y_i|$

#### MAPE 平均绝对百分误差(Mean Absolute Percentage Error)

$MAPE=\dfrac{100}{N}\sum_{i=1}^{N}|\dfrac{f_i-y_i}{y_i}|$

#### explained variance 解释方差

$\mathrm{ExplainedVariance}=\dfrac{SSR}{N}$

### 使用方式

该组件通常接回归预测算法的输出端。

使用时，需要通过参数 `labelCol` 指定预测标签列，通过参数 `predictionCol` 指定预测结果列。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, 0],
    [8, 8],
    [1, 2],
    [9, 10],
    [3, 1],
    [10, 7]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='pred int, label int')

metrics = EvalRegressionBatchOp().setPredictionCol("pred").setLabelCol("label").linkFrom(inOp).collectMetrics()

print("Total Samples Number:", metrics.getCount())
print("SSE:", metrics.getSse())
print("SAE:", metrics.getSae())
print("RMSE:", metrics.getRmse())
print("R2:", metrics.getR2())
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalRegressionBatchOpTest {
	@Test
	public void testEvalRegressionBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, 0),
			Row.of(8, 8),
			Row.of(1, 2),
			Row.of(9, 10),
			Row.of(3, 1),
			Row.of(10, 7)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "pred int, label int");
		RegressionMetrics metrics = new EvalRegressionBatchOp().setPredictionCol("pred").setLabelCol("label").linkFrom(
			inOp).collectMetrics();
		System.out.println("Total Samples Number:" + metrics.getCount());
		System.out.println("SSE:" + metrics.getSse());
		System.out.println("SAE:" + metrics.getSae());
		System.out.println("RMSE:" + metrics.getRmse());
		System.out.println("R2:" + metrics.getR2());
	}
}
```

### 运行结果
```
Total Samples Number: 6.0
SSE: 15.0
SAE: 7.0
RMSE: 1.5811388300841898
R2: 0.8282442748091603
```

