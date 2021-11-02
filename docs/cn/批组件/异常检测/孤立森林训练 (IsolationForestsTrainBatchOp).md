# 孤立森林训练 (IsolationForestsTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.IsolationForestsTrainBatchOp

Python 类名：IsolationForestsTrainBatchOp


## 功能介绍

isolationforests是一种常用的树模型，在异常检测中常常可以取得很好的效果

[Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  | 2147483647 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  | 2147483647 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  | 2 |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  | 10 |
| seed | 采样种子 | 采样种子 | Long |  | 0 |
| subsamplingRatio | 每棵树的样本采样比例或采样行数 | 每棵树的样本采样比例或采样行数，行数上限100w行 | Double |  | 100000.0 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, 0],
    [2.0, 1],
    [3.0, 2],
    [4.0, 3]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr='f0 double, f1 double')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr='f0 double, f1 double')

trainOp = IsolationForestsTrainBatchOp()\
    .setFeatureCols(['f0', 'f1'])\
    .linkFrom(batchSource)
predictBatchOp = IsolationForestsPredictBatchOp()\
    .setPredictionCol('pred')
predictStreamOp = IsolationForestsPredictStreamOp(trainOp)\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IsolationForestsPredictBatchOp;
import com.alibaba.alink.operator.batch.outlier.IsolationForestsTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.outlier.IsolationForestsPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IsolationForestsTrainBatchOpTest {
	@Test
	public void testIsolationForestsTrainBatchOp() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of(1.0, 0.0),
			Row.of(2.0, 1.0),
			Row.of(3.0, 2.0)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(sourceFrame, "f0 double, f1 double");
		StreamOperator <?> streamSource = new MemSourceStreamOp(sourceFrame, "f0 double, f1 double");
		BatchOperator <?> trainOp =
			new IsolationForestsTrainBatchOp().setFeatureCols("f0", "f1").linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new IsolationForestsPredictBatchOp().setPredictionCol("pred");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		StreamOperator <?> predictStreamOp = new IsolationForestsPredictStreamOp(trainOp).setPredictionCol("pred");
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

批预测结果

f0|f1|pred
---|---|----
1.0000|0.0000|0.2816
2.0000|1.0000|0.2816
3.0000|2.0000|0.2816

流预测结果

f0|f1|pred
---|---|----
2.0000|1.0000|0.2816
3.0000|2.0000|0.2816
1.0000|0.0000|0.2816

