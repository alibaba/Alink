# 广义线性回归评估 (GlmEvaluationBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp

Python 类名：GlmEvaluationBatchOp


## 功能介绍
使用新数据集，对GLM模型进行评估

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| epsilon | 收敛精度 | 收敛精度 | Double |  |  | 1.0E-5 |
| family | 分布族 | 分布族，包含gaussian, Binomial, Poisson, Gamma and Tweedie，默认值gaussian。 | String |  | "Gamma", "Binomial", "Gaussian", "Poisson", "Tweedie" | "Gaussian" |
| fitIntercept | 是否拟合常数项 | 是否拟合常数项，默认是拟合 | Boolean |  |  | true |
| link | 连接函数 | 连接函数，包含cloglog, Identity, Inverse, log, logit, power, probit和sqrt，默认值是指数分布族对应的连接函数。 | String |  | "CLogLog", "Identity", "Inverse", "Log", "Logit", "Power", "Probit", "Sqrt" | null |
| linkPower | 连接函数的超参 | 连接函数的超参 | Double |  |  | 1.0 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  |  | 10 |
| offsetCol | 偏移列 | 偏移列 | String |  |  | null |
| regParam | l2正则系数 | l2正则系数 | Double |  |  | 0.0 |
| variancePower | 分布族的超参 | 分布族的超参，默认值是0.0 | Double |  |  | 0.0 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |



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




