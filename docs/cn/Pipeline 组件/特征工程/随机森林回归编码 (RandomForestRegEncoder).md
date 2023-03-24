# 随机森林回归编码 (RandomForestRegEncoder)
Java 类名：com.alibaba.alink.pipeline.feature.RandomForestRegEncoder

Python 类名：RandomForestRegEncoder


## 功能介绍

使用随机森林回归模型，对数据进行特征转换。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |  |
| createTreeMode | 创建树的模式。 | series表示每个单机创建单颗树，parallel表示并行创建单颗树。 | String |  |  | "series" |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  |  | null |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  |  | 128 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  |  | 2147483647 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  |  | 2147483647 |
| maxMemoryInMB | 树模型中用来加和统计量的最大内存使用数 | 树模型中用来加和统计量的最大内存使用数 | Integer |  |  | 64 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  |  | 0.0 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  |  | 2 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numSubsetFeatures | 每棵树的特征采样数目 | 每棵树的特征采样数目 | Integer |  |  | 2147483647 |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  | [1, +inf) | 10 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| seed | 采样种子 | 采样种子 | Long |  |  | 0 |
| subsamplingRatio | 每棵树的样本采样比例或采样行数 | 每棵树的样本采样比例或采样行数，行数上限100w行 | Double |  |  | 100000.0 |
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

batchSource = BatchOperator.fromDataframe(\
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(\
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')

randomforestRegEncoderModel = RandomForestRegEncoder()\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .setPredictionCol("encoded_features")\
    .fit(batchSource)

randomforestRegEncoderModel.transform(batchSource).print()
randomforestRegEncoderModel.transform(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.feature.RandomForestRegEncoder;
import com.alibaba.alink.pipeline.feature.RandomForestRegEncoderModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RandomForestRegEncoderTest {
	@Test
	public void testRandomForestRegEncoder() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(df, " f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator <?> streamSource = new MemSourceStreamOp(df, " f0 double, f1 string, f2 int, f3 int, label "
			+ "int");

		RandomForestRegEncoderModel randomforestRegEncoderModel = new RandomForestRegEncoder()
			.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
			.setPredictionCol("encoded_features")
			.fit(batchSource);
		randomforestRegEncoderModel.transform(batchSource).print();
		randomforestRegEncoderModel.transform(streamSource).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f1|f2|f3|label|encoded_features
---|---|---|---|-----|----------------
1.0000|A|0|0|0|$18$0:1.0 2:1.0 4:1.0 5:1.0 7:1.0 9:1.0 11:1.0 13:1.0 15:1.0 17:1.0
2.0000|B|1|1|0|$18$0:1.0 2:1.0 4:1.0 5:1.0 7:1.0 9:1.0 11:1.0 13:1.0 15:1.0 17:1.0
3.0000|C|2|2|1|$18$1:1.0 3:1.0 4:1.0 6:1.0 8:1.0 10:1.0 12:1.0 14:1.0 16:1.0 17:1.0
4.0000|D|3|3|1|$18$1:1.0 3:1.0 4:1.0 6:1.0 8:1.0 10:1.0 12:1.0 14:1.0 16:1.0 17:1.0
