# 随机森林训练 (RandomForestTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp

Python 类名：RandomForestTrainBatchOp


## 功能介绍

- 随机森林是一种常用的树模型，由于bagging的过程，可以避免过拟合

- 随机森林组件支持稠密数据格式

- 支持带样本权重的训练

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |
| createTreeMode | 创建树的模式。 | series表示每个单机创建单颗树，parallel表示并行创建单颗树。 | String |  | "series" |
| featureSubsamplingRatio | 每棵树特征采样的比例 | 每棵树特征采样的比例，范围为(0, 1]。 | Double |  | 0.2 |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  | 128 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  | 2147483647 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  | 2147483647 |
| maxMemoryInMB | 树模型中用来加和统计量的最大内存使用数 | 树模型中用来加和统计量的最大内存使用数 | Integer |  | 64 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  | 0.0 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  | 2 |
| numSubsetFeatures | 每棵树的特征采样数目 | 每棵树的特征采样数目 | Integer |  | 2147483647 |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  | 10 |
| numTreesOfGini | 模型中Cart树的棵数 | 模型中Cart树的棵数 | Integer |  | null |
| numTreesOfInfoGain | 模型中Id3树的棵数 | 模型中Id3树的棵数 | Integer |  | null |
| numTreesOfInfoGainRatio | 模型中C4.5树的棵数 | 模型中C4.5树的棵数 | Integer |  | null |
| subsamplingRatio | 每棵树的样本采样比例或采样行数 | 每棵树的样本采样比例或采样行数，行数上限100w行 | Double |  | 100000.0 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |


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
batchSource = BatchOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')

trainOp = RandomForestTrainBatchOp()\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .linkFrom(batchSource)
predictBatchOp = RandomForestPredictBatchOp()\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')
predictStreamOp = RandomForestPredictStreamOp(trainOp)\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.RandomForestPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RandomForestTrainBatchOpTest {
	@Test
	public void testRandomForestTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(
			df, " f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator <?> streamSource = new MemSourceStreamOp(
			df, " f0 double, f1 string, f2 int, f3 int, label int");
		BatchOperator <?> trainOp = new RandomForestTrainBatchOp()
			.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");
		StreamOperator <?> predictStreamOp = new RandomForestPredictStreamOp(trainOp)
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f1|f2|f3|label|pred|pred_detail
---|---|---|---|-----|----|-----------
1.0000|A|0|0|0|0|{"0":1.0,"1":0.0}
2.0000|B|1|1|0|0|{"0":1.0,"1":0.0}
3.0000|C|2|2|1|1|{"0":0.0,"1":1.0}
4.0000|D|3|3|1|1|{"0":0.0,"1":1.0}

