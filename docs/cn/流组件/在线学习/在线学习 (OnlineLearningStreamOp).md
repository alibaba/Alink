# 在线学习 (OnlineLearningStreamOp)
Java 类名：com.alibaba.alink.operator.stream.onlinelearning.OnlineLearningStreamOp

Python 类名：OnlineLearningStreamOp


## 功能介绍
该组件是一个基于Pipeline模型的在线学习算法组件，支持多种在线学习模型，具体包括（LogisticRegression，SVM，LinearReg，FmClassifier，FmRegressor，Softmax），
并且支持多种在线优化算法，具体包括（Ftrl，SGD，ADAM，MOMENTUM，RMSProp，ADAGRAD）。另外该组件通过Pipeline模型Rebase的方式支持特征工程模型和在线学习模型同步更新。

### 算法原理
该框架将特征工程模型和在线学习模型做成一个Pipeline模型，通过一个批式训练任务，训练这个离线Pipeline模型，然后以该模型为初始模型进行在线学习，
在线学习过程中，将使用最新的流式数据实时更新在线学习模型，并定时输出Pipeline模型。输出的Pipeline模型中特征工程模型保持不变，在线学学习模型实时更新。特征工程模型的更新通过整个Pipeline模型的rebase来完成，
使用者通过定时调度一个批式训练任务训练一个Pipeline模型的方式定时rebase模型，保证特征工程算法的时效性。
### 算法使用
该算法的使用方式分如下几个步骤：
#### 1. 离线训练Pipeline模型
使用大规模离线数据，训练一个离线模型，作为在线学习的初始模型，示例代码如下：
```python    
    pipelineModel = Pipeline() \
	   .add(StandardScaler() \
                    .setSelectedCols(FEATURES) \
           .add(FeatureHasher() \
                    .setNumFeatures(numHashFeatures) \
                    .setSelectedCols(FEATURES) \
                    .setReservedCols(labelColName) \
                    .setOutputCol(vecColName)) \
           .add(FmClassifier()\
                    .setVectorCol(vecColName)\
                    .setLabelCol(labelColName)\
                    .setWithIntercept(True)\
                    .setNumEpochs(2)\
                    .setPredictionCol("pred")\
                    .setPredictionDetailCol("details")).fit(batchData)
```
其中StandardScaler和FeatureHasher 是特征工程模型，这边可以是任何其他特征工程模型，类似GbdtEncoder、OneHot、MultiHot等。
FmClassifier 是在线学习模型，此处还可以是SVM，LogisticRegression，LinearReg，FmRegressor，Softmax 等。
这些特征工程和在线学习模型使用可以参考对应的批式算法的文档。
#### 2. 实时训练Pipeline模型
使用实时训练数据，实时训练在线模型，示例代码如下：
```
    models = OnlineLearningStreamOp(pipelineModel)\
                .setModelStreamFilePath(REBASE_PATH)\
                .setTimeInterval(10)\
                .setLearningRate(0.01)\
                .setOptimMethod("ADAM")\
                .linkFrom(trainStream)
```
其中，REBASE_PATH是用来做rebase的模型流目录，在第3步中将介绍怎么准备Rebase模型。
#### 3. 准备Rebase模型
使用批式数据训练模型，并写出到rebase的模型流目录，代码示例如下：    
```
    pipelineModel = Pipeline() \
        .add(StandardScaler() \
                    .setSelectedCols(FEATURES) \
        .add(FeatureHasher() \
                    .setNumFeatures(numHashFeatures) \
                    .setSelectedCols(FEATURES) \
                    .setReservedCols(labelColName) \
                    .setOutputCol(vecColName)) \
        .add(FmClassifier()\
                    .setVectorCol(vecColName)\
                    .setLabelCol(labelColName)\
                    .setWithIntercept(True)\
                    .setNumEpochs(2)\
                    .setPredictionCol("pred")\
                    .setPredictionDetailCol("details")).fit(batchData)
    pipelineModel.save().link(AppendModelStreamFileSinkBatchOp().setFilePath(REBASE_PATH))
    BatchOperator.execute()
```

- 在线学习系列算法在点击率预估，推荐等领域应用广泛。
- 该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献
[1] McMahan, H. Brendan, et al. "Ad click prediction: a view from the trenches." Proceedings of the 19th ACM SIGKDD international conference on Knowledge discovery and data mining. 2013.

[2] Ruder, Sebastian. "An overview of gradient descent optimization algorithms." arXiv preprint arXiv:1609.04747 (2016).

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| alpha | 希腊字母：阿尔法 | 经常用来表示算法特殊的参数 | Double |  |  | 0.1 |
| beta | 希腊字母：贝塔 | 经常用来表示算法特殊的参数 | Double |  |  | 1.0 |
| beta1 | beta1 | beta1: parameter for adam optimizer. | Double |  | [0.0, 1.0] | 0.9 |
| beta2 | beta2 | beta2: parameter for adam optimizer. | Double |  | [0.0, 1.0] | 0.999 |
| gamma | gamma | gamma: parameter for RMSProp or momentum optimizer. | Double |  | [0.0, 1.0] | 0.9 |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0.1。 | Double |  | [0.0, +inf) | 0.1 |
| l2 | 正则化系数 | L2 正则化系数，默认为0.1。 | Double |  | [0.0, +inf) | 0.1 |
| learningRate | 学习率 | 优化算法的学习率，默认0.1。 | Double |  |  | null |
| optimMethod | 优化方法 | 在线学习问题求解时选择的优化方法 | String |  | "FTRL", "ADAM", "RMSprop", "ADAGRAD", "SGD", "MOMENTUM" | "FTRL" |
| timeInterval | 时间间隔 | 数据流流动过程中时间的间隔 | Integer |  |  | 1800 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
FEATURE_LABEL = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "label"]
FEATURES = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"]
labelColName = "label"
vecColName ="vec"
REBASE_PATH = "/tmp/rebase"
trainStream = RandomTableSourceStreamOp()\
    .setNumCols(10)\
    .setMaxRows(100000)\
    .setOutputCols(FEATURE_LABEL)\
    .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")\
    .link(SpeedControlStreamOp().setTimeInterval(0.01))

testStream = trainStream

batchData = RandomTableSourceBatchOp()\
    .setNumCols(10)\
    .setNumRows(1000)\
    .setOutputCols(FEATURE_LABEL)\
    .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")

pipelineModel = Pipeline()\
    .add(OneHotEncoder()\
         .setSelectedCols(FEATURES)\
         .setOutputCols([vecColName])\
         .setReservedCols([labelColName]))\
    .add(FmClassifier()\
         .setVectorCol(vecColName)\
         .setLabelCol(labelColName)\
         .setWithIntercept(True)\
         .setNumEpochs(2)\
         .setPredictionCol("pred")\
         .setPredictionDetailCol("details")).fit(batchData)

models = OnlineLearningStreamOp(pipelineModel)\
    .setModelStreamFilePath(REBASE_PATH)\
    .setTimeInterval(10)\
    .setLearningRate(0.01)\
    .setOptimMethod("ADAM")\
    .linkFrom(trainStream)

predResults = PipelinePredictStreamOp(pipelineModel).linkFrom(testStream, models)

EvalBinaryClassStreamOp()\
    .setPredictionDetailCol("details")\
    .setLabelCol(labelColName)\
    .setTimeInterval(10).linkFrom(predResults)\
    .link(JsonValueStreamOp().setSelectedCol("Data")\
          .setReservedCols(["Statistics"])\
          .setOutputCols(["Accuracy", "AUC", "ConfusionMatrix"])\
          .setJsonPath(["$.Accuracy", "$.AUC", "ConfusionMatrix"])).print()

StreamOperator.execute()

```
### Java 代码
```java
package benchmark.online;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkUnimplementedOperationException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.dataproc.SpeedControlStreamOp;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalMultiClassStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalRegressionStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.OnlineLearningStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.PipelinePredictStreamOp;
import com.alibaba.alink.operator.stream.sink.ModelStreamFileSinkStreamOp;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams.OptimMethod;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.pipeline.classification.FmClassifier;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.Softmax;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.FeatureHasher;
import com.alibaba.alink.pipeline.feature.GbdtEncoder;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import com.alibaba.alink.pipeline.regression.FmRegressor;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import org.junit.Test;

public class OnlineLearningTest {

	private static final String[] FEATURES = new String[] {
		"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"
	};
	private static final String[] FEATURE_LABEL = new String[] {
		"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "label"
	};

	private static final String labelColName = "label";
	private static final String vecColName = "vec";
	private static final String REBASE_PATH = "/tmp/rebase";
	private static final String MODEL_PATH = "/tmp/encoder_web";
	private static final String PIPELINE_PATH = "/tmp/pipeline_model.ak";

	private static Tuple2 <StreamOperator, StreamOperator> getStreamTrainData() {
		StreamOperator source = new RandomTableSourceStreamOp()
			.setNumCols(10)
			.setMaxRows(100000L)
			.setOutputCols(FEATURE_LABEL)
			.setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")
			.link(new SpeedControlStreamOp().setTimeInterval(0.01));
		SplitStreamOp splitter = new SplitStreamOp().setFraction(0.5);
		source.link(splitter);
		return new Tuple2 <>(splitter, splitter.getSideOutput(0));
	}

	private static BatchOperator getBatchSet() {
		return new RandomTableSourceBatchOp()
			.setNumCols(10)
			.setNumRows(1000L)
			.setOutputCols(FEATURE_LABEL)
			.setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)");
	}

	@Test
	public void ClassificationOnlineTrainAndEval() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Tuple2 <StreamOperator, StreamOperator> sources = getStreamTrainData();
		StreamOperator trainStream = sources.f0;
		StreamOperator testStream = sources.f1;

		PipelineModel pipelineModel = getFeaturePipeline(EncoderType.GBDT)
			.add(getLastPipelineStage(StageType.LR))
			.fit(getBatchSet());

		BatchOperator pipelineModelData = pipelineModel.save();
		StreamOperator.setParallelism(8);

		StreamOperator models = new OnlineLearningStreamOp(pipelineModelData)
			.setOptimMethod(OptimMethod.ADAGRAD)
			.setLearningRate(0.1)
			.setTimeInterval(10)
			.linkFrom(trainStream);

		StreamOperator predResults = new PipelinePredictStreamOp(pipelineModel)
			.linkFrom(testStream, models);

		new EvalBinaryClassStreamOp()
			.setPredictionDetailCol("details").setLabelCol(labelColName).setTimeInterval(10).linkFrom(predResults)
			.link(new JsonValueStreamOp().setSelectedCol("Data")
				.setReservedCols(new String[] {"Statistics"})
				.setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
				.setJsonPath("$.Accuracy", "$.AUC", "ConfusionMatrix")).print();
		StreamOperator.execute();
	}

	@Test
	public void regressionOnlineTrainAndEval() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Tuple2 <StreamOperator, StreamOperator> sources = getStreamTrainData();
		StreamOperator trainStream = sources.f0;
		StreamOperator testStream = sources.f1;

		PipelineModel pipelineModel = getFeaturePipeline(EncoderType.FEATURE_HASH)
			.add(getLastPipelineStage(StageType.FM_REG))
			.fit(getBatchSet());

		BatchOperator pipelineModelData = pipelineModel.save();
		StreamOperator.setParallelism(2);

		StreamOperator models = new OnlineLearningStreamOp(pipelineModelData)
			.setTimeInterval(10)
			.linkFrom(trainStream);

		StreamOperator predResults = new PipelinePredictStreamOp(pipelineModel)
			.linkFrom(testStream, models);

		new EvalRegressionStreamOp()
			.setPredictionCol("pred").setLabelCol(labelColName).setTimeInterval(10).linkFrom(predResults)
			.link(new JsonValueStreamOp().setSelectedCol("regression_eval_result")
				.setReservedCols(new String[] {})
				.setOutputCols(new String[] {"MAPE", "RMSE", "MAE"})
				.setJsonPath("$.MAPE", "$.RMSE", "$.MAE"))
			.print();

		StreamOperator.execute();
	}

	@Test
	public void saveOnlineModels() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Tuple2 <StreamOperator, StreamOperator> sources = getStreamTrainData();
		StreamOperator trainStream = sources.f0;

		PipelineModel pipelineModel = getFeaturePipeline(EncoderType.ONE_HOT)
			.add(getLastPipelineStage(StageType.LR)).fit(getBatchSet());

		BatchOperator pipelineModelData = pipelineModel.save();
		StreamOperator.setParallelism(2);

		StreamOperator models = new OnlineLearningStreamOp(pipelineModelData)
			.setTimeInterval(10)
			.setModelStreamFilePath(REBASE_PATH)
			.linkFrom(trainStream);
		models.link(new ModelStreamFileSinkStreamOp().setNumKeepModel(10).setFilePath(MODEL_PATH));

		StreamOperator.execute();
	}

	@Test
	public void OnlineSoftmaxTrainAndEval() throws Exception {
		BatchOperator batchData = new RandomTableSourceBatchOp()
			.setNumCols(10)
			.setNumRows(1000L)
			.setOutputCols(FEATURE_LABEL)
			.setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0,3.0,3.0)");

		StreamOperator <?> streamData = new RandomTableSourceStreamOp()
			.setNumCols(10)
			.setMaxRows(100000L)
			.setOutputCols(FEATURE_LABEL)
			.setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0,3.0,3.0)")
			.link(new SpeedControlStreamOp().setTimeInterval(0.01))
			.link(new SplitStreamOp(0.5));

		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PipelineModel pipelineModel
			= getFeaturePipeline(EncoderType.ASSEMBLER)
			.add(getLastPipelineStage(StageType.SOFTMAX))
			.fit(batchData);

		StreamOperator.setParallelism(2);
		StreamOperator models = new OnlineLearningStreamOp(pipelineModel)
			.setOptimMethod(OptimMethod.ADAM)
			.setLearningRate(0.01)
			.setTimeInterval(10)
			.linkFrom(streamData);

		StreamOperator predResults = new PipelinePredictStreamOp(pipelineModel)
			.linkFrom(streamData.getSideOutput(0), models);

		new EvalMultiClassStreamOp()
			.setPredictionDetailCol("details").setLabelCol(labelColName).setTimeInterval(10).linkFrom(predResults)
			.link(new JsonValueStreamOp().setSelectedCol("Data")
				.setReservedCols(new String[] {"Statistics"})
				.setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
				.setJsonPath("$.Accuracy", "$.AUC", "ConfusionMatrix")).print();

		StreamOperator.execute();
	}

	@Test
	public void saveRebaseModel() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PipelineModel pipelineModel = getFeaturePipeline(EncoderType.GBDT)
			.add(getLastPipelineStage(StageType.LR)).fit(getBatchSet());
		BatchOperator pipelineModelData = pipelineModel.save();
		pipelineModelData.link(new AppendModelStreamFileSinkBatchOp().setFilePath(REBASE_PATH));
		BatchOperator.execute();
	}

	@Test
	public void savePipelineModel() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PipelineModel pipelineModel = getFeaturePipeline(EncoderType.ONE_HOT)
			.add(getLastPipelineStage(StageType.LR)).fit(getBatchSet());
		BatchOperator pipelineModelData = pipelineModel.setModelStreamFilePath(MODEL_PATH).save();
		pipelineModelData.link(new AkSinkBatchOp().setOverwriteSink(true).setFilePath(PIPELINE_PATH));
		BatchOperator.execute();
	}

	enum StageType {
		LR,
		LINEAR_REG,
		FM_CLASSIFIER,
		FM_REG,
		SOFTMAX,
		SVM
	}

	private PipelineStageBase getLastPipelineStage(StageType type) {
		switch (type) {
			case LR:
				return new LogisticRegression()
					.setVectorCol(vecColName)
					.setLabelCol(labelColName)
					.setMaxIter(1)
					.setWithIntercept(true)
					.setPredictionCol("pred")
					.setPredictionDetailCol("details");
			case SVM:
				return new LinearSvm()
					.setVectorCol(vecColName)
					.setLabelCol(labelColName)
					.setMaxIter(1)
					.setWithIntercept(true)
					.setPredictionCol("pred")
					.setPredictionDetailCol("details");
			case FM_CLASSIFIER:
				return new FmClassifier()
					.setVectorCol(vecColName)
					.setLabelCol(labelColName)
					.setNumEpochs(1)
					.setWithIntercept(true)
					.setWithIntercept(true)
					.setNumFactor(10).setPredictionCol("pred")
					.setPredictionDetailCol("details");
			case FM_REG:
				new FmRegressor()
					.setVectorCol(vecColName)
					.setLabelCol(labelColName)
					.setNumEpochs(1)
					.setWithIntercept(true)
					.setWithIntercept(true)
					.setNumFactor(10).setPredictionCol("pred")
					.setPredictionDetailCol("details");
			case LINEAR_REG:
				return new LinearRegression()
					.setVectorCol(vecColName)
					.setLabelCol(labelColName)
					.setMaxIter(1)
					.setWithIntercept(true)
					.setPredictionCol("pred");
			case SOFTMAX:
				return new Softmax()
					.setVectorCol(vecColName)
					.setLabelCol(labelColName)
					.setMaxIter(100)
					.setWithIntercept(true)
					.setPredictionCol("pred")
					.setPredictionDetailCol("details");
			default:
				throw new AkUnimplementedOperationException("not support yet.");
		}
	}

	enum EncoderType {
		ONE_HOT,
		FEATURE_HASH,
		ASSEMBLER,
		GBDT
	}

	private Pipeline getFeaturePipeline(EncoderType type) {
		Pipeline pipeline = new Pipeline();
		int numHashFeatures = 30000;
		switch (type) {
			case FEATURE_HASH:
				pipeline
					.add(
						new StandardScaler()
							.setSelectedCols(FEATURES))
					.add(
						new FeatureHasher()
							.setNumFeatures(numHashFeatures)
							.setSelectedCols(FEATURES)
							.setReservedCols(labelColName)
							.setOutputCol(vecColName));
				break;
			case ONE_HOT:
				pipeline
					.add(
						new OneHotEncoder()
							.setSelectedCols(FEATURES)
							.setOutputCols(vecColName)
							.setReservedCols(labelColName)
							.setReservedCols(labelColName));
				break;
			case ASSEMBLER:
				pipeline
					.add(
						new StandardScaler()
							.setSelectedCols(FEATURES))
					.add(
						new VectorAssembler()
							.setSelectedCols(FEATURES)
							.setReservedCols(labelColName)
							.setOutputCol(vecColName));
				break;
			case GBDT:
				pipeline
					.add(
						new StandardScaler()
							.setSelectedCols(FEATURES))
					.add(
						new GbdtEncoder()
							.setLabelCol(labelColName)
							.setFeatureCols(FEATURES)
							.setReservedCols(labelColName)
							.setPredictionCol(vecColName));
				break;
			default:
		}

		return pipeline;
	}
}
```
### 运行结果
Statistics| Accuracy           | AUC  | ConfusionMatrix                    
----------|--------------------|------|------------------------------------
all|0.8404921700223713|0.5297035062366982|[[3757,713],[0,0]]
window|0.8404921700223713|0.5297035062366982|[[3757,713],[0,0]]
window|0.32543617998163454|0.5001458256936807|[[1076,204],[3469,696]]
all|0.5576399394856278|0.5128750935507584|[[4833,917],[3469,696]]
window|0.6837857666911226|0.4987630054677052|[[3547,711],[1013,181]]
all|0.6023947419795666|0.5015687641976192|[[8380,1628],[4482,877]]
...| ...                | ...  | ...                                | 
