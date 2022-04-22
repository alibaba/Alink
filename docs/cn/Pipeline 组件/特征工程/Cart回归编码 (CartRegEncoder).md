# Cart回归编码 (CartRegEncoder)
Java 类名：com.alibaba.alink.pipeline.feature.CartRegEncoder

Python 类名：CartRegEncoder


## 功能介绍

使用Cart回归模型，对数据进行特征转换。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |  |
| createTreeMode | 创建树的模式。 | series表示每个单机创建单颗树，parallel表示并行创建单颗树。 | String |  |  | "series" |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  |  | 128 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  |  | 2147483647 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  |  | 2147483647 |
| maxMemoryInMB | 树模型中用来加和统计量的最大内存使用数 | 树模型中用来加和统计量的最大内存使用数 | Integer |  |  | 64 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  |  | 0.0 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  |  | 2 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
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
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])

batchSource = BatchOperator.fromDataframe(df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')

cartRegEncoderModel = CartRegEncoder()\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .setPredictionCol("encoded_features")\
    .fit(batchSource)

cartRegEncoderModel.transform(batchSource).print()
cartRegEncoderModel.transform(streamSource).print()

StreamOperator.execute()
```
### Java代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.feature.CartRegEncoder;
import com.alibaba.alink.pipeline.feature.CartRegEncoderModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CartRegEncoderTest {
	@Test
	public void testCartRegEncoder() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(df, " f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator <?> streamSource = new MemSourceStreamOp(df, " f0 double, f1 string, f2 int, f3 int, label "
			+ "int");
		CartRegEncoderModel cartRegEncoderModel = new CartRegEncoder()
    		.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
    		.setPredictionCol("encoded_features")
    		.fit(batchSource);

		cartRegEncoderModel.transform(batchSource).print();
		cartRegEncoderModel.transform(streamSource).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
f0|f1|f2|f3|label|encoded_features
---|---|---|---|-----|----------------
1.0000|A|0|0|0|$2$0:1.0
2.0000|B|1|1|0|$2$0:1.0
3.0000|C|2|2|1|$2$1:1.0
4.0000|D|3|3|1|$2$1:1.0
