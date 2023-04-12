# KerasSequential分类器 (KerasSequentialClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.KerasSequentialClassifier

Python 类名：KerasSequentialClassifier


## 功能介绍

构建一个 Keras 的 [Sequential 模型](https://www.tensorflow.org/versions/r2.3/api_docs/python/tf/keras/Sequential )，
训练分类模型。

通过 layers 参数指定构成 Sequential 模型的网络层，Alink 会自动在最开始添加 Input 层，在最后添加 Dense 层和激活层，得到完整的模型用于训练。

指定 layers 参数时，使用的是 Python 语句，例如
```python
"Conv1D(256, 5, padding='same', activation='relu')",
"Conv1D(128, 5, padding='same', activation='relu')",
"Dropout(0.1)",
"MaxPooling1D(pool_size=8)",
"Conv1D(128, 5, padding='same', activation='relu')",
"Conv1D(128, 5, padding='same', activation='relu')",
"Flatten()"
```
tf.keras.layers 内的网络层已经提前 import，可以直接使用。
使用的 TensorFlow 版本是 2.3.1。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| layers | 各 layer 的描述 | 各 layer 的描述，使用 Python 语法，例如 "Conv1D(256, 5, padding='same', activation='relu')" | String[] | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| tensorCol | tensor列 | tensor列 | String | ✓ |  |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  |  | 128 |
| bestMetric | 最优指标 | 判断模型最优时用的指标，仅在总并发度为 1 时起作用。都支持的有：loss； 二分类还支持：auc, precision, recall, binary_accuracy, false_negatives, false_positives, true_negatives, true_positives；多分类还支持：sparse_categorical_accuracy；回归还支持：mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, mean_squared_logarithmic_error, root_mean_squared_error | String |  |  | "loss" |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  |  | null |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  |  | 256 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  |  | 0.001 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numEpochs | epoch数 | epoch数 | Integer |  |  | 10 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  |  | null |
| optimizer | 优化器 | 优化器，使用 Python 语法，例如 "Adam(learning_rate=0.1)" | String |  |  | "Adam()" |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| saveBestOnly | 是否导出最优的 checkpoint | 是否导出最优的 checkpoint，仅在总并发度为 1 时生效 | Boolean |  |  | false |
| saveCheckpointsEpochs | 每隔多少 epochs 保存 checkpoints | 每隔多少 epochs 保存 checkpoints | Double |  |  | 1.0 |
| saveCheckpointsSecs | 每隔多少秒保存 checkpoints | 每隔多少秒保存 checkpoints | Double |  |  |  |
| validationSplit | 验证集比例 | 验证集比例，仅在总并发度为 1 时生效 | Double |  |  | 0.0 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
source = CsvSourceBatchOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv") \
    .setSchemaStr("tensor string, label int")

source = ToTensorBatchOp() \
    .setSelectedCol("tensor") \
    .setTensorDataType("DOUBLE") \
    .setTensorShape([200, 3]) \
    .linkFrom(source)

trainer = KerasSequentialClassifier() \
    .setTensorCol("tensor") \
    .setLabelCol("label") \
    .setLayers([
        "Conv1D(256, 5, padding='same', activation='relu')",
        "Conv1D(128, 5, padding='same', activation='relu')",
        "Dropout(0.1)",
        "MaxPooling1D(pool_size=8)",
        "Conv1D(128, 5, padding='same', activation='relu')",
        "Conv1D(128, 5, padding='same', activation='relu')",
        "Flatten()"
    ]) \
    .setOptimizer("Adam()") \
    .setNumEpochs(1) \
    .setPredictionCol("pred") \
    .setPredictionDetailCol("pred_detail") \
    .setReservedCols(["label"])

model = trainer.fit(source)
prediction = model.transform(source)
prediction.lazyPrint(10)
BatchOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToTensorBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.KerasSequentialClassificationModel;
import com.alibaba.alink.pipeline.classification.KerasSequentialClassifier;
import org.junit.Test;

public class KerasSequentialClassifierTest {

	@Test
	public void testKerasSequentialClassifier() throws Exception {
		BatchOperator<?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label int");

		source = new ToTensorBatchOp()
			.setSelectedCol("tensor")
			.setTensorDataType("DOUBLE")
			.setTensorShape(200, 3)
			.linkFrom(source);

		KerasSequentialClassifier trainer = new KerasSequentialClassifier()
			.setTensorCol("tensor")
			.setLabelCol("label")
			.setLayers(new String[] {
				"Conv1D(256, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Dropout(0.1)",
				"MaxPooling1D(pool_size=8)",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Flatten()"
			})
			.setOptimizer("Adam()")
			.setNumEpochs(1)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("label");

		KerasSequentialClassificationModel model = trainer.fit(source);
		BatchOperator <?> prediction = model.transform(source);
		prediction.lazyPrint(10);
		BatchOperator.execute();
	}
}
```

### 运行结果

| label | pred | pred_detail                                      |
|-------|------|--------------------------------------------------|
| 0     | 0    | {"0":0.636155836712713,"1":0.36384416328728697}  |
| 1     | 0    | {"0":0.6334926095655181,"1":0.3665073904344819}  |
| 1     | 0    | {"0":0.6381823204965642,"1":0.3618176795034358}  |
| 1     | 0    | {"0":0.6376416296248051,"1":0.362358370375195}   |
| 1     | 0    | {"0":0.6345856985385896,"1":0.36541430146141035} |
| 1     | 0    | {"0":0.6357593109428179,"1":0.364240689057182}   |
| 0     | 0    | {"0":0.6404387449594703,"1":0.3595612550405296}  |
| 1     | 0    | {"0":0.6372702905549685,"1":0.36272970944503136} |
| 0     | 0    | {"0":0.635502012172225,"1":0.36449798782777487}  |
| 0     | 0    | {"0":0.6262401788033837,"1":0.37375982119661644} |
