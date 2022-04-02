# Isotonic回归 (IsotonicRegression)
Java 类名：com.alibaba.alink.pipeline.regression.IsotonicRegression

Python 类名：IsotonicRegression


## 功能介绍

保序回归在观念上是寻找一组非递减的片段连续线性函数（piecewise linear continuous functions），即保序函数，使其与样本尽可能的接近。

保序回归的输入在Alink中称分别为特征（feature）、标签（label）和权重（weight），特征可以是数值或向量，如果是向量还需要设定特征索引
（feature index），组件将使用该维进行计算。保序回归的目标是求解一个能使$\textstyle \sum_i{w_i(y_i-\hat{y}_i)^2}$最小的序列$\hat{y}$，
若选择保增序，该序列还应满足$X_i<X_j$时$\hat{y}_i\le\hat{y}_j$，若选择保降序满足$X_i<X_j$时$\hat{y}_i\ge\hat{y}_j$。
下图中，散点图是训练数据，折线图是得到的保序回归模型，对于训练数据中没有的特征，使用线性插值得到其标签。对应训练和预测代码见示例。
![isotonic.png](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/17357000/1642558915236-d33cb8f3-3ea8-4282-a621-e7ff50c443db.png#clientId=u97fe9e46-e3c1-4&crop=0&crop=0.0889&crop=1&crop=1&from=ui&height=360&id=ua83fdaf8&margin=%5Bobject%20Object%5D&name=isotonic.png&originHeight=960&originWidth=1280&originalType=binary&ratio=1&rotation=0&showTitle=false&size=42838&status=done&style=none&taskId=u70106ba9-a7fd-4136-adbb-a8b04b196f3&title=&width=480)

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| featureCol | 特征列名 | 特征列的名称 | String |  | null |
| isotonic | 输出序列是否 | 输出序列是否递增 | Boolean |  | true |
| featureIndex | 训练特征所在维度 | 训练特征在输入向量的维度索引 | Integer |  | 0 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0.35, 1],
    [0.6, 1],
    [0.55, 1],
    [0.5, 1],
    [0.18, 0],
    [0.1, 1],
    [0.8, 1],
    [0.45, 0],
    [0.4, 1],
    [0.7, 0],
    [0.02, 1],
    [0.3, 0],
    [0.27, 1],
    [0.2, 0],
    [0.9, 1]])

data = BatchOperator.fromDataframe(df, schemaStr="label double, feature double")

res = IsotonicRegression()\
            .setFeatureCol("feature")\
            .setLabelCol("label")\
            .setPredictionCol("result")

res.fit(data).transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.regression.IsotonicRegression;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IsotonicRegressionTest {
	@Test
	public void testIsotonicRegression() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0.02, 0.0),
			Row.of(0.1, 0.0),
			Row.of(0.18, 1.0),
			Row.of(0.2, 0.0),
			Row.of(0.27, 1.0),
			Row.of(0.3, 0.0),
			Row.of(0.35, 1.0),
			Row.of(0.4, 1.0),
			Row.of(0.45, 0.0),
			Row.of(0.5, 1.0),
			Row.of(0.55, 1.0),
			Row.of(0.6, 1.0),
			Row.of(0.7, 0.0),
			Row.of(0.8, 1.0),
			Row.of(0.9, 1.0),
			Row.of(0.98, 1.10)
		);
		List <Row> pred = Arrays.asList(
			Row.of(0.2),
			Row.of(0.32),
			Row.of(0.4),
			Row.of(0.45),
			Row.of(0.65),
			Row.of(0.9)
		);

		BatchOperator <?> data = new MemSourceBatchOp(df, "feature double, label double");
		BatchOperator <?> predData = new MemSourceBatchOp(pred, "feature double");
		IsotonicRegressionModel res = new IsotonicRegression()
			.setFeatureCol("feature")
			.setLabelCol("label")
			.setPredictionCol("predict")
			.fit(data);
		res.transform(predData).print();
	}
}
```

### 运行结果
    
| feature | predict |
| --- | --- |
| 0.2000 | 0.5000 |
| 0.3200 | 0.5667 |
| 0.4000 | 0.6667 |
| 0.4500 | 0.6667 |
| 0.6500 | 0.7500 |
| 0.9000 | 1.0000 |
