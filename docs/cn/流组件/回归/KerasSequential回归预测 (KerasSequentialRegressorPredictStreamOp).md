# KerasSequential回归预测 (KerasSequentialRegressorPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.KerasSequentialRegressorPredictStreamOp

Python 类名：KerasSequentialRegressorPredictStreamOp


## 功能介绍

与 KerasSequential 回归训练组件对应的流预测组件。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  | 256 |
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

streamSource = CsvSourceStreamOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv") \
    .setSchemaStr("tensor string, label int")

trainBatchOp = KerasSequentialRegressorTrainBatchOp() \
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
    .linkFrom(source)

predictStreamOp = KerasSequentialRegressorPredictStreamOp(trainBatchOp) \
    .setPredictionCol("pred") \
    .setReservedCols(["label"]) \
    .linkFrom(streamSource)
predictStreamOp.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorTrainBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.KerasSequentialRegressorPredictStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class KerasSequentialRegressorPredictStreamOpTest {

	@Test
	public void testKerasSequentialRegressorPredictStreamOp() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
		pluginDownloader.downloadPlugin("tf231_python_env_macosx"); // change according to system type
		pluginDownloader.downloadPlugin("tf_predictor_macosx"); // change according to system type

		BatchOperator <?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label int");

		StreamOperator <?> streamSource = new CsvSourceStreamOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label int");

		KerasSequentialRegressorTrainBatchOp trainBatchOp = new KerasSequentialRegressorTrainBatchOp()
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
			.linkFrom(source);

		KerasSequentialRegressorPredictStreamOp predictStreamOp = new KerasSequentialRegressorPredictStreamOp(trainBatchOp)
			.setPredictionCol("pred")
			.setReservedCols("label")
			.linkFrom(streamSource);
		predictStreamOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

label|pred|pred_detail
-----|----|-----------
0|0|{"0":0.636155836712713,"1":0.36384416328728697}
1|0|{"0":0.6334926095655181,"1":0.3665073904344819}
1|0|{"0":0.6381823204965642,"1":0.3618176795034358}
1|0|{"0":0.6376416296248051,"1":0.362358370375195}
1|0|{"0":0.6345856985385896,"1":0.36541430146141035}
1|0|{"0":0.6357593109428179,"1":0.364240689057182}
0|0|{"0":0.6404387449594703,"1":0.3595612550405296}
1|0|{"0":0.6372702905549685,"1":0.36272970944503136}
0|0|{"0":0.635502012172225,"1":0.36449798782777487}
0|0|{"0":0.6262401788033837,"1":0.37375982119661644}
