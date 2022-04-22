# 流式K均值聚类 (StreamingKMeansStreamOp)
Java 类名：com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp

Python 类名：StreamingKMeansStreamOp


## 功能介绍
流式Kmeans聚类算法，对流数据进行Kmeans聚类。流式KMeans聚类，需要三个输入: 
1. 训练好的批式的KMeans模型
2. 流式的更新模型的数据
3. 流式的需要预测的数据

若只有两个输入，那么第一个输入被算法识别为训练好的初始Kmeans模型，第二个输入被同时用作"流式的更新模型的数据"和"流式的需要预测的数据"。

本算法组件会根据2流入的数据在固定的timeinterval内更新模型，这个模型会用来预测3的输入数据。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| halfLife | 半生命周期 | 半生命周期 | Integer | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| timeInterval | 时间间隔 | 时间间隔，单位秒 | Long | ✓ |  |  |
| predictionClusterCol | 预测距离列名 | 预测距离列名 | String |  |  |  |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |





## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
  [0, "0 0 0"],
  [1, "0.1,0.1,0.1"],
  [2, "0.2,0.2,0.2"],
  [3, "9 9 9"],
  [4, "9.1 9.1 9.1"],
  [5, "9.2 9.2 9.2"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
stream_data = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')

init_model = KMeansTrainBatchOp()\
    .setVectorCol("vec")\
    .setK(2)\
    .linkFrom(inOp)

streamingkmeans = StreamingKMeansStreamOp(init_model) \
  .setTimeInterval(1) \
  .setHalfLife(1) \
  .setReservedCols(["vec"])

pred = streamingkmeans.linkFrom(stream_data, stream_data)

pred.print()
StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StreamingKMeansStreamOpTest {
	@Test
	public void testStreamingKMeansStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(2, "0.2,0.2,0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		StreamOperator <?> stream_data = new MemSourceStreamOp(df, "id int, vec string");
		BatchOperator <?> init_model = new KMeansTrainBatchOp()
			.setVectorCol("vec")
			.setK(2)
            .linkFrom(inOp);
		StreamOperator <?> streamingkmeans = new StreamingKMeansStreamOp(init_model)
			.setTimeInterval(1L)
			.setHalfLife(1)
			.setReservedCols("vec");
		StreamOperator <?> pred = streamingkmeans.linkFrom(stream_data, stream_data);
		pred.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
vec|cluster_id
---|----------
0.2,0.2,0.2|1
0 0 0|1
0.1,0.1,0.1|1
9.2 9.2 9.2|0
9.1 9.1 9.1|0
9 9 9|0
