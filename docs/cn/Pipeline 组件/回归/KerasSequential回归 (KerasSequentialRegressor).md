# KerasSequential回归 (KerasSequentialRegressor)
Java 类名：com.alibaba.alink.pipeline.regression.KerasSequentialRegressor

Python 类名：KerasSequentialRegressor


## 功能介绍

构建一个 Keras 的 [Sequential 模型](https://www.tensorflow.org/versions/r2.3/api_docs/python/tf/keras/Sequential )，
训练回归模型。

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
```tf.keras.layers``` 内的网络层已经提前 import，可以直接使用。
使用的 TensorFlow 版本是 2.3.1。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| layers | 各 layer 的描述 | 各 layer 的描述，使用 Python 语法，例如 "Conv1D(256, 5, padding='same', activation='relu')" | String[] | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| tensorCol | Not available! | Not available! | String | ✓ |  |
| validationSplit | 验证集比例 | 验证集比例，当前需要总并发度为 1 | Double |  | 0.0 |
| saveBestOnly | 是否导出最优的 checkpoint | 是否导出最优的 checkpoint | Boolean |  | false |
| saveCheckpointsEpochs | 每隔多少 epochs 保存 checkpoints | 每隔多少 epochs 保存 checkpoints | Double |  | 1.0 |
| saveCheckpointsSecs | 每隔多少秒保存 checkpoints | 每隔多少秒保存 checkpoints | Double |  |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  | 128 |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  | null |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  | 256 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  | 0.001 |
| numEpochs | epoch数 | epoch数 | Integer |  | 10 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  | null |
| optimizer | 优化器 | 优化器，使用 Python 语法，例如 "Adam(learning_rate=0.1)" | String |  | "Adam()" |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。
 如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；
 如果是目录，那么只能使用本地路径，即 file://。 | String |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader()
pluginDownloader.downloadPlugin("tf231_python_env_macosx") # change according to system type
pluginDownloader.downloadPlugin("tf_predictor_macosx") # change according to system type

source = CsvSourceBatchOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv") \
    .setSchemaStr("tensor string, label double")

trainer = KerasSequentialRegressor() \
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
    .setReservedCols(["label"])

model = trainer.fit(source)
prediction = model.transform(source)
prediction.lazyPrint(10)
BatchOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.regression.KerasSequentialRegressionModel;
import com.alibaba.alink.pipeline.regression.KerasSequentialRegressor;
import org.junit.Test;

public class KerasSequentialRegressorTest {

	@Test
	public void testKerasSequentialRegressor() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
		pluginDownloader.downloadPlugin("tf231_python_env_macosx"); // change according to system type
		pluginDownloader.downloadPlugin("tf_predictor_macosx"); // change according to system type

		BatchOperator<?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label double");

		KerasSequentialRegressor trainer = new KerasSequentialRegressor()
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
			.setReservedCols("label");

		KerasSequentialRegressionModel model = trainer.fit(source);
		BatchOperator <?> prediction = model.transform(source);
		prediction.lazyPrint(10);
		BatchOperator.execute();
	}
}
```

### 运行结果

label|pred
-----|----
0.0000|0.4580
1.0000|0.4323
1.0000|0.4547
1.0000|0.4381
0.0000|0.4361
1.0000|0.4633
0.0000|0.4565
1.0000|0.4928
1.0000|0.4306
0.0000|0.4359
