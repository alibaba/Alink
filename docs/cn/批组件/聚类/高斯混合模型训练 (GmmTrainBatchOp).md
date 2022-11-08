# 高斯混合模型训练 (GmmTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.GmmTrainBatchOp

Python 类名：GmmTrainBatchOp


## 功能介绍
混合模型（Mixture Model）是一个可以用来表示在总体分布中含有K个子分布的概率模型。换句话说，混合模型表示了观测数据在总体中的概率分布，它是一个由K个子分布组成的混合分布。
而高斯混合模型（Gaussian Mixture Model, GMM）可以用来表示在总体分布中含有K个高斯子分布的概率模型。它通常可以被用作分类模型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  |  | 1.0E-4 |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  |  | 0 |

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

public class GmmTrainBatchOpTest {
	@Test
	public void testGmmTrainBatchOp() throws Exception {
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
-0.6264538 0.1836433|1|4.275273913968281E-92 1.0
-0.8356286 1.5952808|1|1.0260377730239899E-92 1.0
0.3295078 -0.8204684|1|1.0970173367545207E-80 1.0
0.4874291 0.7383247|1|3.302173132311E-75 1.0
0.5757814 -0.3053884|1|3.1638113605165424E-76 1.0
1.5117812 0.3898432|1|2.101805230873173E-62 1.0
-0.6212406 -2.2146999|1|6.772270268600749E-97 1.0
11.1249309 9.9550664|0|1.0 3.156783801247968E-56
9.9838097 10.9438362|0|1.0 1.9024447346702425E-51
10.8212212 10.5939013|0|1.0 2.800973098729604E-56
10.9189774 10.7821363|0|1.0 1.7209132744891298E-57
10.0745650 8.0106483|0|1.0 2.8642696635130495E-43
10.6198257 9.9438713|0|1.0 5.773273991940433E-53
9.8442045 8.5292476|0|1.0 2.5273123050925483E-43
9.5218499 10.4179416|0|1.0 1.7314580596767853E-46


## 备注
### 资源如何预估
- 每个worker的内存大小如何估计？
  - 假设聚类中心点数量为K，输入数据的维度为m，那么每个worker需要的memory可以设置为（m \* m \* k \* 8 \* 2 \* 12) ~ 200km\^2，转换为MB之后是（200km\^2 / 1024 / 1024). 一般来说，设置每个worker 8GB即可。
- 如何设置worker的个数？
  - 建议按照输入数据的大小设置。假设输入数据为{X}GB，那么建议使用{5X}个worker。如果资源不够使用，可以适当降低worker数目。随着worker数目的增加，由于通信开销的存在，分布式训练任务会先变快，然后变慢。用户如果观测到worker数目增加之后，速度变慢，那么应该停止增加worker数目。
- 本算法可以支持多大的数据量？
    - 建议向量维度小于200。
