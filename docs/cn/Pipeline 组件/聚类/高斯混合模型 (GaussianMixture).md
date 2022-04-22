# 高斯混合模型 (GaussianMixture)
Java 类名：com.alibaba.alink.pipeline.clustering.GaussianMixture

Python 类名：GaussianMixture


## 功能介绍
高斯混合模型聚类

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |  |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  |  | 1.0E-4 |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  |  | 0 |
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

df = pd.DataFrame([
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

data = BatchOperator.fromDataframe(df, schemaStr='features string')

gmm = GaussianMixture() \
    .setPredictionCol("cluster_id") \
    .setVectorCol("features") \
    .setPredictionDetailCol("cluster_detail") \
    .setEpsilon(0.)

gmm.fit(data).transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.clustering.GaussianMixture;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GaussianMixtureTest {
	@Test
	public void testGaussianMixture() throws Exception {
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
		GaussianMixture gmm = new GaussianMixture()
			.setPredictionCol("cluster_id")
			.setVectorCol("features")
			.setPredictionDetailCol("cluster_detail")
			.setEpsilon(0.);
		gmm.fit(data).transform(data).print();
	}
}
```

### 运行结果
features|cluster_id|cluster_detail
--------|----------|--------------
-0.6264538 0.1836433|0|1.0 4.275273913968281E-92
-0.8356286 1.5952808|0|1.0 1.0260377730239899E-92
0.3295078 -0.8204684|0|1.0 1.0970173367545207E-80
0.4874291 0.7383247|0|1.0 3.302173132311E-75
0.5757814 -0.3053884|0|1.0 3.1638113605165424E-76
1.5117812 0.3898432|0|1.0 2.101805230873172E-62
-0.6212406 -2.2146999|0|1.0 6.772270268600749E-97
11.1249309 9.9550664|1|3.156783801247968E-56 1.0
9.9838097 10.9438362|1|1.9024447346702425E-51 1.0
10.8212212 10.5939013|1|2.800973098729604E-56 1.0
10.9189774 10.7821363|1|1.7209132744891298E-57 1.0
10.0745650 8.0106483|1|2.8642696635130495E-43 1.0
10.6198257 9.9438713|1|5.773273991940433E-53 1.0
9.8442045 8.5292476|1|2.5273123050925483E-43 1.0
9.5218499 10.4179416|1|1.7314580596767853E-46 1.0
