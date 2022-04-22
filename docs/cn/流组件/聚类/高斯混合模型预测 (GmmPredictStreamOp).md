# 高斯混合模型预测 (GmmPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.clustering.GmmPredictStreamOp

Python 类名：GmmPredictStreamOp


## 功能介绍
基于GaussianMixture模型进行聚类预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
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

df_data = pd.DataFrame([
    ["-0.6264538 0.1836433"],
    ["-0.8356286 1.5952808"],
    ["0.3295078 -0.8204684"],
    ["0.4874291 0.7383247"],
    ["0.5757814 -0.3053884"],
    ["1.5117812 0.3898432"],
    ["-0.6212406 -2.2146999"],
    ["11.1249309 9.9550664"],
    ["9.9838097 10.9438362"],
    ["10.8212212 10.5939013"],
    ["10.9189774 10.7821363"],
    ["10.0745650 8.0106483"],
    ["10.6198257 9.9438713"],
    ["9.8442045 8.5292476"],
    ["9.5218499 10.4179416"],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='features string')
dataStream = StreamOperator.fromDataframe(df_data, schemaStr='features string')

gmm = GmmTrainBatchOp() \
    .setVectorCol("features") \
    .setEpsilon(0.)

model = gmm.linkFrom(data)

predictor = GmmPredictBatchOp() \
    .setPredictionCol("cluster_id") \
    .setVectorCol("features") \
    .setPredictionDetailCol("cluster_detail")

predictor.linkFrom(model, data).print()

predictorStream = GmmPredictStreamOp(model) \
    .setPredictionCol("cluster_id") \
    .setVectorCol("features") \
    .setPredictionDetailCol("cluster_detail")

predictorStream.linkFrom(dataStream).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.GmmPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.GmmTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.GmmPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GmmPredictStreamOpTest {
	@Test
	public void testGmmPredictStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("-0.6264538 0.1836433"),
			Row.of("-0.8356286 1.5952808"),
			Row.of("0.3295078 -0.8204684"),
			Row.of("0.4874291 0.7383247"),
			Row.of("0.5757814 -0.3053884"),
			Row.of("1.5117812 0.3898432"),
			Row.of("-0.6212406 -2.2146999"),
			Row.of("11.1249309 9.9550664"),
			Row.of("9.9838097 10.9438362"),
			Row.of("10.8212212 10.5939013"),
			Row.of("10.9189774 10.7821363"),
			Row.of("10.0745650 8.0106483"),
			Row.of("10.6198257 9.9438713"),
			Row.of("9.8442045 8.5292476"),
			Row.of("9.5218499 10.4179416")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "features string");
		StreamOperator <?> dataStream = new MemSourceStreamOp(df_data, "features string");
		BatchOperator <?> gmm = new GmmTrainBatchOp()
			.setVectorCol("features")
			.setEpsilon(0.);
		BatchOperator <?> model = gmm.linkFrom(data);
		BatchOperator <?> predictor = new GmmPredictBatchOp()
			.setPredictionCol("cluster_id")
			.setVectorCol("features")
			.setPredictionDetailCol("cluster_detail");
		predictor.linkFrom(model, data).print();
		StreamOperator <?> predictorStream = new GmmPredictStreamOp(model)
			.setPredictionCol("cluster_id")
			.setVectorCol("features")
			.setPredictionDetailCol("cluster_detail");
		predictorStream.linkFrom(dataStream).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

features|cluster_id|cluster_detail
--------|----------|--------------
-0.6264538 0.1836433|1|4.2752739140319505E-92 1.0
-0.8356286 1.5952808|1|1.0260377730418951E-92 1.0
0.3295078 -0.8204684|1|1.097017336765683E-80 1.0
0.4874291 0.7383247|1|3.3021731323490106E-75 1.0
0.5757814 -0.3053884|1|3.163811360548103E-76 1.0
1.5117812 0.3898432|1|2.1018052308895397E-62 1.0
-0.6212406 -2.2146999|1|6.772270268679667E-97 1.0
11.1249309 9.9550664|0|1.0 3.1567838012477056E-56
9.9838097 10.9438362|0|1.0 1.9024447346702016E-51
10.8212212 10.5939013|0|1.0 2.800973098729602E-56
10.9189774 10.7821363|0|1.0 1.7209132744891742E-57
10.0745650 8.0106483|0|1.0 2.8642696635133805E-43
10.6198257 9.9438713|0|1.0 5.773273991940741E-53
9.8442045 8.5292476|0|1.0 2.527312305092764E-43
9.5218499 10.4179416|0|1.0 1.7314580596765114E-46
