# XGBoost 回归预测 (XGBoostRegPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.XGBoostRegPredictBatchOp

Python 类名：XGBoostRegPredictBatchOp


## 功能介绍
XGBoost 组件是在开源社区的基础上进行包装，使功能和 PAI 更兼容，更易用。
XGBoost 算法在 Boosting 算法的基础上进行了扩展和升级，具有较好的易用性和鲁棒性，被广泛用在各种机器学习生产系统和竞赛领域。
当前支持分类，回归和排序。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| pluginVersion | 插件版本号 | 插件版本号 | String |  |  | "1.5.1" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码

```python
df = pd.DataFrame([
    [0, 1, 1.1, 1.0],
    [1, -2, 0.9, 2.0],
    [0, 100, -0.01, 3.0],
    [1, -99, 0.1, 4.0],
    [0, 1, 1.1, 5.0],
    [1, -2, 0.9, 6.0]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr='y int, x1 double, x2 double, x3 double'
)

streamSource = StreamOperator.fromDataframe(
    df, schemaStr='y int, x1 double, x2 double, x3 double'
)

trainOp = XGBoostRegTrainBatchOp()\
    .setNumRound(1)\
    .setPluginVersion('1.5.1')\
    .setLabelCol('y')\
    .linkFrom(batchSource)

predictBatchOp = XGBoostRegPredictBatchOp()\
    .setPredictionCol('pred')\
    .setPluginVersion('1.5.1')

predictStreamOp = XGBoostRegPredictStreamOp(trainOp)\
    .setPredictionCol('pred')\
    .setPluginVersion('1.5.1')

predictBatchOp.linkFrom(trainOp, batchSource).print()

predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.XGBoostRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.XGBoostRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.XGBoostRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class XGBoostRegTrainBatchOpTest {

	@Test
	public void testXGBoostTrainBatchOp() throws Exception {
		List <Row> data = Arrays.asList(
			Row.of(0, 1, 1.1, 1.0),
			Row.of(1, -2, 0.9, 2.0),
			Row.of(0, 100, -0.01, 3.0),
			Row.of(1, -99, 0.1, 4.0),
			Row.of(0, 1, 1.1, 5.0),
			Row.of(1, -2, 0.9, 6.0)
		);

		BatchOperator <?> batchSource = new MemSourceBatchOp(data, "y int, x1 int, x2 double, x3 double");
		StreamOperator <?> streamSource = new MemSourceStreamOp(data, "y int, x1 int, x2 double, x3 double");
		BatchOperator <?> trainOp = new XGBoostRegTrainBatchOp()
			.setNumRound(1)
			.setPluginVersion("1.5.1")
			.setLabelCol("y")
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new XGBoostRegPredictBatchOp()
			.setPredictionCol("pred")
			.setPluginVersion("1.5.1");
		StreamOperator <?> predictStreamOp = new XGBoostRegPredictStreamOp(trainOp)
			.setPredictionCol("pred")
			.setPluginVersion("1.5.1");

		predictBatchOp.linkFrom(trainOp, batchSource).print();

		predictStreamOp.linkFrom(streamSource).print();

		StreamOperator.execute();
	}
}
```
### 运行结果
