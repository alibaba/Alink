# CART决策树回归训练 (CartRegTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp

Python 类名：CartRegTrainBatchOp


## 功能介绍

- cart回归是一种常用的树模型

- cart回归组件支持稠密数据格式

- 支持带样本权重的训练

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ | 所选列类型为 [BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, LONG, SHORT, STRING, TIME, TIMESTAMP] |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  | 所选列类型为 [BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, LONG, SHORT, STRING, TIME, TIMESTAMP] |  |
| createTreeMode | 创建树的模式。 | series表示每个单机创建单颗树，parallel表示并行创建单颗树。 | String |  |  | "series" |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  |  | 128 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  |  | 2147483647 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  |  | 2147483647 |
| maxMemoryInMB | 树模型中用来加和统计量的最大内存使用数 | 树模型中用来加和统计量的最大内存使用数 | Integer |  |  | 64 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  |  | 0.0 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  |  | 2 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |



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
    df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int')

trainOp = CartRegTrainBatchOp()\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .linkFrom(batchSource)
predictBatchOp = CartRegPredictBatchOp()\
    .setPredictionCol('pred')
predictStreamOp = CartRegPredictStreamOp(trainOp)\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.CartRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.CartRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CartRegTrainBatchOpTest {
	@Test
	public void testCartRegTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(
			df, "f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator <?> streamSource = new MemSourceStreamOp(
			df, "f0 double, f1 string, f2 int, f3 int, label int");
		BatchOperator <?> trainOp = new CartRegTrainBatchOp()
			.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new CartRegPredictBatchOp()
			.setPredictionCol("pred");
		StreamOperator <?> predictStreamOp = new CartRegPredictStreamOp(trainOp)
			.setPredictionCol("pred");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```


### 运行结果

#### 批预测结果
f0|f1|f2|f3|label|pred|
---|---|---|---|---|---|
1.0000|A|0|0|0|0.0000|
2.0000|B|1|1|0|0.0000|
3.0000|C|2|2|1|1.0000|
4.0000|D|3|3|1|1.0000|

#### 流预测结果

f0|f1|f2|f3|label|pred
---|---|---|---|-----|----
1.0000|A|0|0|0|0.0000
4.0000|D|3|3|1|1.0000
3.0000|C|2|2|1|1.0000
2.0000|B|1|1|0|0.0000

