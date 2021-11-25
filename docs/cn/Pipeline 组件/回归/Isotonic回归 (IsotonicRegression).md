# Isotonic回归 (IsotonicRegression)
Java 类名：com.alibaba.alink.pipeline.regression.IsotonicRegression

Python 类名：IsotonicRegression


## 功能介绍

保序回归在观念上是寻找一组非递减的片段连续线性函数（piecewise linear continuous functions），即保序函数，使其与样本尽可能的接近。
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
			Row.of(0.35, 1.0),
			Row.of(0.6, 1.0),
			Row.of(0.55, 1.0),
			Row.of(0.5, 1.0),
			Row.of(0.18, 0.0),
			Row.of(0.1, 1.0),
			Row.of(0.8, 1.0),
			Row.of(0.45, 0.0),
			Row.of(0.4, 1.0),
			Row.of(0.7, 0.0),
			Row.of(0.02, 1.0),
			Row.of(0.3, 0.0),
			Row.of(0.27, 1.0),
			Row.of(0.2, 0.0),
			Row.of(0.9, 1.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "label double, feature double");
		IsotonicRegression res = new IsotonicRegression()
			.setFeatureCol("feature")
			.setLabelCol("label")
			.setPredictionCol("result");
		res.fit(data).transform(data).print();
	}
}
```

### 运行结果
    
label|feature|result
-----|-------|------
0.3500|1.0000|0.3611
0.6000|1.0000|0.3611
0.5500|1.0000|0.3611
0.5000|1.0000|0.3611
0.1800|0.0000|0.3611
0.1000|1.0000|0.3611
0.8000|1.0000|0.3611
0.4500|0.0000|0.3611
0.4000|1.0000|0.3611
0.7000|0.0000|0.3611
0.0200|1.0000|0.3611
0.3000|0.0000|0.3611
0.2700|1.0000|0.3611
0.2000|0.0000|0.3611
0.9000|1.0000|0.3611
