# 广义线性回归 (GeneralizedLinearRegression)
Java 类名：com.alibaba.alink.pipeline.regression.GeneralizedLinearRegression

Python 类名：GeneralizedLinearRegression


## 功能介绍
广义线性回归训练和预测

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| epsilon | 收敛精度 | 收敛精度 | Double |  |  | 1.0E-5 |
| family | 分布族 | 分布族，包含gaussian, Binomial, Poisson, Gamma and Tweedie，默认值gaussian。 | String |  | "Gamma", "Binomial", "Gaussian", "Poisson", "Tweedie" | "Gaussian" |
| fitIntercept | 是否拟合常数项 | 是否拟合常数项，默认是拟合 | Boolean |  |  | true |
| link | 连接函数 | 连接函数，包含cloglog, Identity, Inverse, log, logit, power, probit和sqrt，默认值是指数分布族对应的连接函数。 | String |  | "CLogLog", "Identity", "Inverse", "Log", "Logit", "Power", "Probit", "Sqrt" | null |
| linkPower | 连接函数的超参 | 连接函数的超参 | Double |  |  | 1.0 |
| linkPredResultCol | 连接函数结果的列名 | 连接函数结果的列名 | String |  |  | null |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  |  | 10 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| offsetCol | 偏移列 | 偏移列 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| regParam | l2正则系数 | l2正则系数 | Double |  |  | 0.0 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| variancePower | 分布族的超参 | 分布族的超参，默认值是0.0 | Double |  |  | 0.0 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |



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
glm = GeneralizedLinearRegression()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)\
                .setPredictionCol("pred")

model = glm.fit(source)
predict = model.transform(source)
eval2 = model.evaluate(source)

predict.lazyPrint(10)
eval2.print()


```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.regression.GeneralizedLinearRegression;
import com.alibaba.alink.pipeline.regression.GeneralizedLinearRegressionModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GeneralizedLinearRegressionTest {
	@Test
	public void testGeneralizedLinearRegression() throws Exception {
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
		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFamily("gamma")
			.setLink("Log")
			.setRegParam(0.3)
			.setMaxIter(5)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("pred");
		GeneralizedLinearRegressionModel model = glm.fit(source);
		BatchOperator <?> predict = model.transform(source);
		BatchOperator <?> eval2 = model.evaluate(source);
		predict.lazyPrint(10);
		eval2.print();
	}
}
```

### 运行结果

#### 预测结果

u|lot1|lot2|offset|weights|pred
---|----|----|------|-------|----
1.6094|118.0000|69.0000|1.0000|2.0000|1.4601
2.3026|58.0000|35.0000|1.0000|2.0000|2.6396
2.7081|42.0000|26.0000|1.0000|2.0000|3.0847
2.9957|35.0000|21.0000|1.0000|2.0000|3.4135
3.4012|27.0000|18.0000|1.0000|2.0000|3.5215
3.6889|25.0000|16.0000|1.0000|2.0000|3.6901
4.0943|21.0000|13.0000|1.0000|2.0000|3.9275
4.3820|19.0000|12.0000|1.0000|2.0000|3.9891
4.6052|18.0000|12.0000|1.0000|2.0000|3.9581


#### 评估结果

|summary
|-------
|{"rank":3,"degreeOfFreedom":6,"residualDegreeOfFreeDom":6,"residualDegreeOfFreedomNull":8,"aic":9702.088569686523,"dispersion":0.016006720896643168,"deviance":0.09638590199190827,"nullDeviance":0.8493577599031792,"coefficients":[0.007797743508544688,-0.031175844426488887],"intercept":1.609524324733498,"coefficientStandardErrors":[0.030385113783611438,0.053017230010619414,0.10937960484662312],"tValues":[0.2566303869742456,-0.5880323136505685,14.715031444760141],"pValues":[0.8060371545112832,0.5779564640151,6.188226474801439E-6]}










