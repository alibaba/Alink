# 随机森林预测 (RandomForestPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.RandomForestPredictBatchOp

Python 类名：RandomForestPredictBatchOp


## 功能介绍

随机森林一种经典的有监督学习非线性决策树模型，可以解决分类，回归和其他的一些决策树模型可以解决的问题，通常可以拿到比单决策树更好的效果。

### 算法原理

通过 Bagging 的方法组合多棵决策树，生成最终的模型。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])
batchSource = BatchOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')

trainOp = RandomForestTrainBatchOp()\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .linkFrom(batchSource)
predictBatchOp = RandomForestPredictBatchOp()\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')
predictStreamOp = RandomForestPredictStreamOp(trainOp)\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.RandomForestPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RandomForestPredictBatchOpTest {
	@Test
	public void testRandomForestPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);

		BatchOperator <?> batchSource = new MemSourceBatchOp(
			df, " f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator <?> streamSource = new MemSourceStreamOp(
			df, " f0 double, f1 string, f2 int, f3 int, label int");
		BatchOperator <?> trainOp = new RandomForestTrainBatchOp()
			.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");
		StreamOperator <?> predictStreamOp = new RandomForestPredictStreamOp(trainOp)
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f1|f2|f3|label|pred|pred_detail
---|---|---|---|-----|----|-----------
1.0000|A|0|0|0|0|{"0":1.0,"1":0.0}
2.0000|B|1|1|0|0|{"0":1.0,"1":0.0}
3.0000|C|2|2|1|1|{"0":0.0,"1":1.0}
4.0000|D|3|3|1|1|{"0":0.0,"1":1.0}

### 算法使用

我们给定 Adult 数据集，在这个场景下介绍随机森林的使用步骤

#### 数据集
[Adult](https://archive.ics.uci.edu/ml/datasets/adult)

##### 训练集

训练数据集的基本统计结果为

Adult train
Summary:

|       colName|count|missing|       sum|      mean|        variance|  min|    max|
|--------------|-----|-------|----------|----------|----------------|-----|-------|
|           age|32560|      0|   1256214|   38.5815|        186.0665|   17|     90|
|     workclass|32560|   1836|       NaN|       NaN|             NaN|  NaN|    NaN|
|        fnlwgt|32560|      0|6179243539|189780.207|11141029667.4508|12285|1484705|
|     education|32560|      0|       NaN|       NaN|             NaN|  NaN|    NaN|
| education_num|32560|      0|    328231|   10.0808|          6.6186|    1|     16|
|marital_status|32560|      0|       NaN|       NaN|             NaN|  NaN|    NaN|
|    occupation|32560|   1843|       NaN|       NaN|             NaN|  NaN|    NaN|
|  relationship|32560|      0|       NaN|       NaN|             NaN|  NaN|    NaN|
|          race|32560|      0|       NaN|       NaN|             NaN|  NaN|    NaN|
|           sex|32560|      0|       NaN|       NaN|             NaN|  NaN|    NaN|
|  capital_gain|32560|      0|  35089324| 1077.6819|   54544178.6998|    0|  99999|
|  capital_loss|32560|      0|   2842700|   87.3065|     162381.6909|    0|   4356|
|hours_per_week|32560|      0|   1316644|   40.4375|        152.4637|    1|     99|
|native_country|32560|    583|       NaN|       NaN|             NaN|  NaN|    NaN|
|         label|32560|      0|       NaN|       NaN|             NaN|  NaN|    NaN|

读取数据可以使用如下方法进行：

```java
CsvSourceBatchOp trainData = new CsvSourceBatchOp()
	.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/adult_train.csv")
	.setIgnoreFirstLine(true)
	.setSchemaStr(schemaStr)
	.lazyPrintStatistics("Adult train");
```

上述代码中可以使用

```java
lazyPrintStatistics("Adult train");
```

即可拿到数据的统计结果

##### 测试集

测试数据集的基本统计结果为

Adult test
Summary:

|       colName|count|missing|       sum|       mean|        variance|  min|    max|
|--------------|-----|-------|----------|-----------|----------------|-----|-------|
|           age|16280|      0|    631146|    38.7682|        191.8033|   17|     90|
|     workclass|16280|    963|       NaN|        NaN|             NaN|  NaN|    NaN|
|        fnlwgt|16280|      0|3083900756|189428.7934|11175556521.7039|13492|1490400|
|     education|16280|      0|       NaN|        NaN|             NaN|  NaN|    NaN|
| education_num|16280|      0|    163987|    10.0729|          6.5927|    1|     16|
|marital_status|16280|      0|       NaN|        NaN|             NaN|  NaN|    NaN|
|    occupation|16280|    966|       NaN|        NaN|             NaN|  NaN|    NaN|
|  relationship|16280|      0|       NaN|        NaN|             NaN|  NaN|    NaN|
|          race|16280|      0|       NaN|        NaN|             NaN|  NaN|    NaN|
|           sex|16280|      0|       NaN|        NaN|             NaN|  NaN|    NaN|
|  capital_gain|16280|      0|  17614497|  1081.9716|   57519546.0031|    0|  99999|
|  capital_loss|16280|      0|   1431088|    87.9047|     162503.3785|    0|   3770|
|hours_per_week|16280|      0|    657586|    40.3923|        155.7433|    1|     99|
|native_country|16280|    274|       NaN|        NaN|             NaN|  NaN|    NaN|
|         label|16280|      0|       NaN|        NaN|             NaN|  NaN|    NaN|

读取数据可以使用如下方法进行：

```java
CsvSourceBatchOp testData = new CsvSourceBatchOp()
	.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/adult_test.csv")
	.setIgnoreFirstLine(true)
	.setSchemaStr(schemaStr)
	.lazyPrintStatistics("Adult test");
```

#### 训练

训练模型可以使用 RandomForestTrainBatchOp ， 其中支持一些常用的决策树剪枝参数，可以通过调整这些参数来拿到一些更好的模型，详细可以参考参数说明部分。

```java
String[] numericalFeatureColNames = new String[] {"age", "fnlwgt", "education_num", "capital_gain",
	"capital_loss", "hours_per_week"};

String[] categoryFeatureColNames = new String[] {"workclass", "education", "marital_status", "occupation",
	"relationship", "race", "sex", "native_country"};

RandomForestTrainBatchOp randomForestBatchOp = new RandomForestTrainBatchOp()
	.setFeatureCols(ArrayUtils.addAll(numericalFeatureColNames, categoryFeatureColNames))
	.setCategoricalCols(categoryFeatureColNames)
	.setSubsamplingRatio(0.6)
	.setMaxLeaves(32)
	.setLabelCol("label");
```

#### 预测

```java
RandomForestPredictBatchOp prediction = new RandomForestPredictBatchOp()
	.setPredictionCol("prediction")
	.setPredictionDetailCol("prediction_detail");
```

#### 评估

```java
EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
	.setLabelCol("prediction")
	.setPredictionDetailCol("prediction_detail");
```

#### 训练预测流程构建

```java
prediction
	.linkFrom(
		randomForestBatchOp
			.linkFrom(trainData)
			.lazyPrintModelInfo("Adult random forest model")
			.lazyCollectModelInfo(new Consumer <RandomForestModelInfo>() {
				@Override
				public void accept(RandomForestModelInfo randomForestModelInfo) {
					try {
						randomForestModelInfo
							.saveTreeAsImage("/tmp/rf_adult_model.png", 0, true);
					} catch (IOException e) {
						throw new IllegalStateException(e);
					}
				}
			}),
		testData
	)
	.link(eval)
	.lazyPrintMetrics("Adult random forest evaluation");
```

#### 执行

```java
BatchOperator.execute();
```

#### 运行结果

##### 模型信息

Adult random forest model
Classification trees modelInfo:
Number of trees: 10
Number of features: 14
Number of categorical features: 8
Labels: [<=50K, >50K]

Categorical feature info:

|       feature|number of categorical value|
|--------------|---------------------------|
|     workclass|                          8|
|     education|                         16|
|marital_status|                          7|
|           ...|                        ...|
|          race|                          5|
|           sex|                          2|
|native_country|                         41|

Table of feature importance Top 14:

|       feature|importance|
|--------------|----------|
|           age|    0.1997|
|        fnlwgt|    0.1992|
|  capital_gain|    0.1447|
|hours_per_week|    0.1091|
| education_num|    0.0889|
|    occupation|    0.0553|
|  relationship|    0.0423|
|  capital_loss|    0.0336|
|     workclass|    0.0306|
|           sex|    0.0299|
|          race|    0.0188|
|marital_status|    0.0176|
|native_country|    0.0158|
|     education|    0.0144|

Classification trees modelInfo:
Number of trees: 10
Number of features: 14
Number of categorical features: 8
Labels: [<=50K, >50K]

Categorical feature info:

|       feature|number of categorical value|
|--------------|---------------------------|
|     workclass|                          8|
|     education|                         16|
|marital_status|                          7|
|           ...|                        ...|
|          race|                          5|
|           sex|                          2|
|native_country|                         41|

Table of feature importance Top 14:

|       feature|importance|
|--------------|----------|
|        fnlwgt|    0.2318|
|           age|    0.2286|
|hours_per_week|    0.1382|
| education_num|    0.0706|
|    occupation|    0.0645|
|  capital_gain|    0.0568|
|     workclass|    0.0516|
|           sex|     0.033|
|  relationship|    0.0299|
|  capital_loss|    0.0222|
|     education|    0.0218|
|native_country|    0.0199|
|          race|    0.0175|
|marital_status|    0.0136|

模型信息中包含一些常用的训练输入数据的基本信息，特征的基本信息，模型的基本信息。

离散特征的一些统计信息，可以通过 Categorical feature info 部分查看。

特征重要性是一类更常用的筛选特征的指标，可以通过 Table of feature importance Top 14 部分查看。

##### 模型可视化

我们也输出了随进森林中第 0 号树的模型结果可视化结果，通过代码中 lazyCollectModelInfo 收集到模型信息之后，通过模型中提供的 saveTreeAsImage ，可以输出模型的图片结果到指定路径。

```
.lazyCollectModelInfo(new Consumer <RandomForestModelInfo>() {
	@Override
	public void accept(RandomForestModelInfo randomForestModelInfo) {
		try {
			randomForestModelInfo
				.saveTreeAsImage("/tmp/rf_adult_model.png", 0, true);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
})
```
<img src="https://img.alicdn.com/imgextra/i1/O1CN01AH5mt41scxAdk1WVV_!!6000000005788-2-tps-20177-2990.png" style="zoom:10%">

##### 评估结果

Adult random forest evaluation
-------------------------------- Metrics: --------------------------------
Auc:1	Accuracy:0.9995	Precision:0.9965	Recall:1	F1:0.9982	LogLoss:0.2584

|Pred\Real|>50K|<=50K|
|---------|----|-----|
|     >50K|2273|    8|
|    <=50K|   0|13999|

### 文献或出处
1. [RandomForest](https://link.springer.com/content/pdf/10.1023/A:1010933404324.pdf)
2. [weka](https://www.cs.waikato.ac.nz/ml/weka/)

