# 广义线性回归预测 (GlmPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.GlmPredictStreamOp

Python 类名：GlmPredictStreamOp


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


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| linkPredResultCol | 连接函数结果的列名 | 连接函数结果的列名 | String |  |  | null |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
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

# data
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

#  batch predict
predict =  GlmPredictBatchOp()\
                .setPredictionCol("pred")

predict.linkFrom(train, source)
predict.print()

# eval
eval =  GlmEvaluationBatchOp()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)

eval.linkFrom(train, source)
eval.print()


# stream predict
source_stream = StreamOperator.fromDataframe(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double')

predict_stream =  GlmPredictStreamOp(train)\
                .setPredictionCol("pred")

predict_stream.linkFrom(source_stream)
predict_stream.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp;
import com.alibaba.alink.operator.batch.regression.GlmPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.GlmPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GlmPredictStreamOpTest {
	@Test
	public void testGlmPredictStreamOp() throws Exception {
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
		predict.print();
		BatchOperator <?> eval = new GlmEvaluationBatchOp()
			.setFamily("gamma")
			.setLink("Log")
			.setRegParam(0.3)
			.setMaxIter(5)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName);
		eval.linkFrom(train, source);
		eval.print();
		StreamOperator <?> source_stream = new MemSourceStreamOp(df,
			"u double, lot1 double, lot2 double, offset double, weights double");
		StreamOperator <?> predict_stream = new GlmPredictStreamOp(train)
			.setPredictionCol("pred");
		predict_stream.linkFrom(source_stream);
		predict_stream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

#### 批预测结果

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

{"rank":3,"degreeOfFreedom":6,"residualDegreeOfFreeDom":6,"residualDegreeOfFreedomNull":8,"aic":9702.088569686532,"dispersion":0.016006720896643168,"deviance":0.0963859019919082,"nullDeviance":0.8493577599031797,"coefficients":[0.007797743508544201,-0.031175844426488047],"intercept":1.609524324733497,"coefficientStandardErrors":[0.030385113783605693,0.05301723001060941,0.10937960484661188],"tValues":[0.25663038697427815,-0.5880323136506637,14.715031444761644],"pValues":[0.8060371545112608,0.5779564640150403,6.188226474801439E-6]}

#### 流预测结果

u|lot1|lot2|offset|weights|pred
---|----|----|------|-------|----
2.7081|42.0000|26.0000|1.0000|2.0000|3.0847
2.9957|35.0000|21.0000|1.0000|2.0000|3.4135
1.6094|118.0000|69.0000|1.0000|2.0000|1.4601
4.0943|21.0000|13.0000|1.0000|2.0000|3.9275
4.3820|19.0000|12.0000|1.0000|2.0000|3.9891
3.4012|27.0000|18.0000|1.0000|2.0000|3.5215
2.3026|58.0000|35.0000|1.0000|2.0000|2.6396
3.6889|25.0000|16.0000|1.0000|2.0000|3.6901
4.6052|18.0000|12.0000|1.0000|2.0000|3.9581




