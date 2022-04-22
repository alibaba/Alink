# 生存回归 (AftSurvivalRegression)
Java 类名：com.alibaba.alink.pipeline.regression.AftSurvivalRegression

Python 类名：AftSurvivalRegression


## 功能介绍
在生存分析领域，加速失效时间模型(accelerated failure time model,AFT 模型)可以作为比例风险模型的替代模型。生存回归组件支持稀疏、稠密两种数据格式。

### 算法原理
AFT模型将线性回归模型的建模方法引人到生存分析的领域， 将生存时间的对数作为反应变量，研究多协变量与对数生存时间之间的回归关系，在形式上，模型与一般的线性回归模型相似。对回归系数的解释也与一般的线性回归模型相似，较之Cox模型， AFT模型对分析结果的解释更加简单、直观且易于理解，并且可以预测个体的生存时间。

### 算法使用
生存回归分析是研究特定事件的发生与时间的关系的回归。这里特定事件可以是：病人死亡、病人康复、用户流失、商品下架等。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献或出处
[1] Wei, Lee-Jen. "The accelerated failure time model: a useful alternative to the Cox regression model in survival analysis." Statistics in medicine 11.14‐15 (1992): 1871-1879.

[2] https://spark.apache.org/docs/latest/ml-classification-regression.html#survival-regression

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| censorCol | 生存列名 | 生存列名 | String | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | [0.0, +inf) | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| quantileProbabilities | 分位数概率数组 | 分位数概率数组 | double[] |  |  | [0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99] |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  |  | true |
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
    [1.218, 1.0, "1.560,-0.605"],
    [2.949, 0.0, "0.346,2.158"],
    [3.627, 0.0, "1.380,0.231"],
    [0.273, 1.0, "0.520,1.151"],
    [4.199, 0.0, "0.795,-0.226"]])

data = BatchOperator.fromDataframe(df, schemaStr="label double, censor double, features string")

reg = AftSurvivalRegression()\
            .setVectorCol("features")\
            .setLabelCol("label")\
            .setCensorCol("censor")\
            .setPredictionCol("result")

pipeline = Pipeline().add(reg)
model = pipeline.fit(data)

model.save().lazyPrint(10)
model.transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.regression.AftSurvivalRegression;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AftSurvivalRegressionTest {
	@Test
	public void testAftSurvivalRegression() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.218, 1.0, "1.560,-0.605"),
			Row.of(2.949, 0.0, "0.346,2.158"),
			Row.of(3.627, 0.0, "1.380,0.231"),
			Row.of(0.273, 1.0, "0.520,1.151")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "label double, censor double, features string");
		AftSurvivalRegression reg = new AftSurvivalRegression()
			.setVectorCol("features")
			.setLabelCol("label")
			.setCensorCol("censor")
			.setPredictionCol("result");
		Pipeline pipeline = new Pipeline().add(reg);
		PipelineModel model = pipeline.fit(data);
		model.save().lazyPrint(10);
		model.transform(data).print();
	}
}
```

### 运行结果

#### 模型

id|p0|p1|p2
---|---|---|---
-1|{"stages":"[{\"identifier\":null,\"params\":null,\"schemaIndices\":[1,0,2],\"colNames\":null,\"parent\":-1},{\"identifier\":\"com.alibaba.alink.pipeline.regression.AftSurvivalRegressionModel\",\"params\":{\"params\":{\"vectorCol\":\"\\\"features\\\"\",\"labelCol\":\"\\\"label\\\"\",\"censorCol\":\"\\\"censor\\\"\",\"predictionCol\":\"\\\"result\\\"\"}},\"schemaIndices\":[1,0,2],\"colNames\":[\"model_id\",\"model_info\",\"label_value\"],\"parent\":0}]"}|null|null
1|{"hasInterceptItem":"true","vectorCol":"\"features\"","modelName":"\"AFTSurvivalRegTrainBatchOp\"","labelCol":"\"label\"","linearModelType":"\"AFT\"","vectorSize":"3"}|0|null
1|{"featureColNames":null,"featureColTypes":null,"coefVector":{"data":[-29.487324590178716,24.42773010344541,13.44725070039797,-1.3679961023031253]},"coefVectors":null,"convergenceInfo":[1.5843984652595493,3.6032723097911044,0.4,1.4794299745122195,0.9580954096270979,1.6,1.3777797119903465,0.7050802052575507,1.6,1.3399286821587995,0.3682693394041936,1.6,1.312648708021441,0.24739884143507995,4.0,1.2685626011340911,0.18750659133206055,4.0,1.253583736945237,0.14860925947925266,4.0,1.2281061305710799,0.14586073980515185,4.0,1.0942468743404496,0.18594588792948594,4.0,0.8350708072737613,0.3504767418363587,4.0,0.8350708072737618,0.5905879812330285,0.25,0.5762249843357561,0.5905879812330285,0.25,0.5161276782605526,0.7008299684632876,0.25,0.3872690319921853,0.6482128538375713,1.0,0.3872690319921861,0.6448719615922804,0.0625,0.3786668764217095,0.6448719615922804,0.0625,0.3354192551580446,3.6845873037311816,0.25,0.26183684259256024,3.141816288006177,1.0,-0.07097230167077419,2.6843478562459735,1.0]}|1048576|null

#### 结果

label|censor|features|result
-----|------|--------|------
1.2180|1.0000|1.560,-0.605|1.6231
2.9490|0.0000|0.346,2.158|2933.1642
3.6270|0.0000|1.380,0.231|1524.2502
0.2730|1.0000|0.520,1.151|0.2706
