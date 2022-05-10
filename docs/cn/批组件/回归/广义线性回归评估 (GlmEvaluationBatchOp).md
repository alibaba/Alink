# 广义线性回归评估 (GlmEvaluationBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp

Python 类名：GlmEvaluationBatchOp


## 功能介绍
GLM(Generalized Linear Model)又称为广义线性回归模型，是一种常用的统计模型，也是一种非线性模型族，许多常用的模型都属于广义线性回归。

它描述了响应和预测因子之间的非线性关系。广义线性回归模型具有线性回归模型的广义特征。响应变量遵循正态、二项式、泊松分布、伽马分布或逆高斯分布，链接函数f定义了μ和预测值的线性组合之间的关系。

GLM功能包括GLM训练，GLM预测(批和流)和GLM评估, 其中训练使用迭代最小二乘方法。

### 算法使用
| 分布 | 连接函数 | 对应算法 |
|:----|:---|:---|
| 二项分布 | Logit | 逻辑回归 |
| 多项分布 | Logit | softmax| 
| 高斯分布 | Identity | 线性回归 | 
| Poisson分布 | Log | Possion回归 |

### 文献或出处
[1] https://en.wikipedia.org/wiki/Generalized_linear_model



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.6094,118.0000,69.0000,1.0000,2.0000],
    [2.3026,58.0000,35.0000,1.0000,2.0000],
    [2.7081,42.0000,26.0000,1.0000,2.0000],
    [2.9957,35.0000,21.0000,1.0000,2.0000],
    [3.4012,27.0000,18.0000,1.0000,2.0000],
    [3.6889,25.0000,16.0000,1.0000,2.0000],
    [4.0943,21.0000,13.0000,1.0000,2.0000],
    [4.3820,19.0000,12.0000,1.0000,2.0000],
    [4.6052,18.0000,12.0000,1.0000,2.0000]
])

source = BatchOperator.fromDataframe(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double')

featureColNames = ["lot1", "lot2"]
labelColName = "u"

# train
train = GlmTrainBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

source.link(train)

# predict
predict =  GlmPredictBatchOp()\
                .setPredictionCol("pred")

predict.linkFrom(train, source)


# eval
eval =  GlmEvaluationBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

eval.linkFrom(train, source)

predict.lazyPrint(10)
eval.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp;
import com.alibaba.alink.operator.batch.regression.GlmPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GlmEvaluationBatchOpTest {
	@Test
	public void testGlmEvaluationBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.6094, 118.0000, 69.0000, 1.0000, 2.0000),
			Row.of(2.3026, 58.0000, 35.0000, 1.0000, 2.0000),
			Row.of(2.7081, 42.0000, 26.0000, 1.0000, 2.0000),
			Row.of(2.9957, 35.0000, 21.0000, 1.0000, 2.0000),
			Row.of(3.4012, 27.0000, 18.0000, 1.0000, 2.0000),
			Row.of(3.6889, 25.0000, 16.0000, 1.0000, 2.0000),
			Row.of(4.0943, 21.0000, 13.0000, 1.0000, 2.0000),
			Row.of(4.3820, 19.0000, 12.0000, 1.0000, 2.0000),
			Row.of(4.6052, 18.0000, 12.0000, 1.0000, 2.0000)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df,
			"u double, lot1 double, lot2 double, offset double, weights double");
		String[] featureColNames = new String[] {"lot1", "lot2"};
		String labelColName = "u";
		BatchOperator <?> train = new GlmTrainBatchOp()
			.setFamily("gamma")
			.setLink("Log")
			.setRegParam(0.3)
			.setMaxIter(5)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName);
		source.link(train);
		BatchOperator <?> predict = new GlmPredictBatchOp()
			.setPredictionCol("pred");
		predict.linkFrom(train, source);
		BatchOperator <?> eval = new GlmEvaluationBatchOp()
			.setFamily("gamma")
			.setLink("Log")
			.setRegParam(0.3)
			.setMaxIter(5)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName);
		eval.linkFrom(train, source);
		predict.lazyPrint(10);
		eval.print();
	}
}
```

### 运行结果

#### 预测结果

 u |  lot1|  lot2 | offset | weights  |    pred
----|----|------|-----------|------|-----------    
0 | 1.6094 | 118.0 | 69.0  |   1.0    |  2.0 | 0.378525
1 | 2.3026 |  58.0 | 35.0  |   1.0  |    2.0 | 0.970639
2|  2.7081  | 42.0 | 26.0 |    1.0  |    2.0 | 1.126458
3 | 2.9957 |  35.0 | 21.0 |    1.0  |    2.0 | 1.227753
4 | 3.4012 |  27.0 | 18.0 |    1.0  |    2.0 | 1.258898
5 | 3.6889 |  25.0 | 16.0 |    1.0  |    2.0 | 1.305654
6 | 4.0943 |  21.0 | 13.0 |    1.0 |     2.0|  1.367991
7 | 4.3820 |  19.0 | 12.0 |    1.0 |     2.0 | 1.383571
8 | 4.6052 |  18.0 | 12.0 |    1.0  |    2.0 | 1.375774


#### 评估结果

|summary
|-------
|{"rank":3,"degreeOfFreedom":6,"residualDegreeOfFreeDom":6,"residualDegreeOfFreedomNull":8,"aic":9702.08856968678,"dispersion":0.01600672089664272,"deviance":0.09638590199190636,"nullDeviance":0.8493577599031797,"coefficients":[0.007797743508551773,-0.031175844426501245],"intercept":1.6095243247335171,"coefficientStandardErrors":[0.030385113783611032,0.05301723001061871,0.10937960484662167],"tValues":[0.2566303869744822,-0.5880323136508093,14.715031444760513],"pValues":[0.8060371545111102,0.5779564640149484,6.188226474801439E-6]}




