# XGBoost二分类训练 (XGBoostClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.XGBoostClassifier

Python 类名：XGBoostClassifier


## 功能介绍
XGBoost 组件是在开源社区的基础上进行包装，使功能和 PAI 更兼容，更易用。
XGBoost 算法在 Boosting 算法的基础上进行了扩展和升级，具有较好的易用性和鲁棒性，被广泛用在各种机器学习生产系统和竞赛领域。
当前支持分类，回归和排序。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| numRound | 树的棵树 | 树的棵树 | Integer | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| alpha | L1 正则项 | L1 正则项 | Double |  |  | 1.0 |
| baseScore | Base score | Base score | Double |  |  | 0.5 |
| colSampleByLevel | 每个树列采样 | 每个树列采样 | Double |  |  | 1.0 |
| colSampleByNode | 每个结点列采样 | 每个结点采样 | Double |  |  | 1.0 |
| colSampleByTree | 每个树列采样 | 每个树列采样 | Double |  |  | 1.0 |
| eta | 学习率 | 学习率 | Double |  |  | 0.3 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  |  | null |
| gamma | 结点分裂最小损失变化 | 节点分裂最小损失变化 | Double |  |  | 0.0 |
| growPolicy | GrowPolicy | GrowPolicy | String |  | "DEPTH_WISE", "LOSS_GUIDE" | "DEPTH_WISE" |
| interactionConstraints | interaction constraints | interaction constraints | String |  |  | null |
| lambda | L2 正则项 | L2 正则项 | Double |  |  | 1.0 |
| maxBin | 最大结点个数 | 最大结点个数 | Integer |  |  | 256 |
| maxDeltaStep | Delta step | Delta step | Double |  |  | 0.0 |
| maxDepth | 最大深度 | 最大深度 | Integer |  |  | 6 |
| maxLeaves | 最大结点个数 | 最大结点个数 | Integer |  |  | 0 |
| minChildWeight | 结点的最小权重 | 结点的最小权重 | Double |  |  | 1.0 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| monotoneConstraints | monotone constraints | monotone constraints | String |  |  | null |
| numClass | 标签类别个数 | 标签类别个数， 多分类时有效 | Integer |  |  | 0 |
| objective | objective | objective | String |  | "BINARY_LOGISTIC", "BINARY_LOGITRAW", "BINARY_HINGE", "MULTI_SOFTMAX", "MULTI_SOFTPROB" | "BINARY_LOGISTIC" |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| pluginVersion | 插件版本号 | 插件版本号 | String |  |  | "1.5.1" |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| processType | ProcessType | ProcessType | String |  | "DEFAULT", "UPDATE" | "DEFAULT" |
| refreshLeaf | RefreshLeaf | RefreshLeaf | Integer |  |  | 1 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| runningMode | 运行模式 | XGBoost的运行模型，ICQ速度快，但使用内存多，TRIAVIAL速度略慢，但是节省内存，按照流式方式处理。由于训练数据本身在XGBoost运行时已经被缓存进内存，所以存两份和存一份数据的资源消耗和速度对比，还需要进一步的测试。 | String |  | "ICQ", "TRIVIAL" | "TRIVIAL" |
| samplingMethod | 采样方法 | 采样方法 | String |  | "UNIFORM", "GRADIENT_BASED" | "UNIFORM" |
| scalePosWeight | ScalePosWeight | ScalePosWeight | Double |  |  | 1.0 |
| singlePrecisionHistogram | single precision histogram | single precision histogram | Boolean |  |  | false |
| sketchEps | SketchEps | SketchEps | Double |  |  | 0.03 |
| subSample | 样本采样比例 | 样本采样比例 | Double |  |  | 1.0 |
| treeMethod | 构建树的方法 | 构建树的方法 | String |  | "AUTO", "EXACT", "APPROX", "HIST" | "AUTO" |
| updater | Updater | Updater | String |  |  | "grow_colmaker,prune" |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

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
    df, schemaStr='y int, x1 int, x2 double, x3 double'
)

streamSource = StreamOperator.fromDataframe(
    df, schemaStr='y int, x1 int, x2 double, x3 double'
)

xgboostClassifier = XGBoostClassifier() \
    .setNumRound(1)\
    .setPluginVersion('1.5.1')\
    .setLabelCol('y')\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')

xgboostClassifier.fit(batchSource).transform(streamSource).print()

StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.classification.XGBoostClassifier;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class XGBoostClassifierTest {

	@Test
	public void testXGBoostClassifier() throws Exception {
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

		XGBoostClassifier xgBoostClassifier = new XGBoostClassifier()
			.setNumRound(1)
			.setPluginVersion("1.5.1")
			.setLabelCol("y")
			.setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred");

		xgBoostClassifier.fit(batchSource).transform(streamSource).print();

		StreamOperator.execute();
	}
}
```
