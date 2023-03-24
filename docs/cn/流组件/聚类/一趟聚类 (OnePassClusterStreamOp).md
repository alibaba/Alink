# 一趟聚类 (OnePassClusterStreamOp)
Java 类名：com.alibaba.alink.operator.stream.clustering.OnePassClusterStreamOp

Python 类名：OnePassClusterStreamOp


## 功能介绍
对流式数据进行one-pass KMeans聚类，数据按批次更新cluster中心点。

本算法组件有两个输入，一个是初始的Kmeans模型，一个是流式数据。第一个输入用来初始化模型，第二个输入用来更新模型，并且输出对应数据点的分类。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| epsilon | 临域距离阈值 | 临域距离阈值 | Double |  |  | 1.7976931348623157E308 |
| modelOutputInterval | 模型输出间隔 | 模型输出间隔，间隔多少条样本输出一个模型 | Integer |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
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

batch_data = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
stream_data = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')

init_model = KMeansTrainBatchOp()\
    .setVectorCol("vec")\
    .setK(2)\
    .linkFrom(batch_data)

onepassCluster = OnePassClusterStreamOp(init_model) \
  .setPredictionCol("pred")\
  .setPredictionDetailCol("distance")\
  .setModelOutputInterval(100)\
  .setEpsilon(1.)\
  .linkFrom(stream_data)
onepassCluster.print()
StreamOperator.execute()

```
### Java 代码
```java

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.OnePassClusterStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OnePassClusterStreamOpTest {
	@Test
	public void testOnePassClusterStreamOp() throws Exception {
		List <Row> dataRows = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(2, "0.2,0.2,0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(dataRows, "id int, vec string");
		StreamOperator <?> streamData = new MemSourceStreamOp(dataRows, "id int, vec string");

		KMeansTrainBatchOp kmeansModel = new KMeansTrainBatchOp()
			.setVectorCol("vec")
			.setK(2)
			.linkFrom(batchData);
		OnePassClusterStreamOp onePassClusterStreamOp = new OnePassClusterStreamOp(kmeansModel)
			.setPredictionCol("pred")
			.setPredictionDetailCol("distance")
			.setModelOutputInterval(100)
			.setEpsilon(1.)
			.linkFrom(streamData);
		onePassClusterStreamOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
id|vec|pred|distance
---|---|----|--------
4|9.1 9.1 9.1|2|0.1732
3|9 9 9|2|0.0000
2|0.2,0.2,0.2|1|0.1732
5|9.2 9.2 9.2|0|0.1732
1|0.1,0.1,0.1|1|0.0231
0|0 0 0|1|0.2454
