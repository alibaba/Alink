# KerasSequential回归预测 (KerasSequentialRegressorPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorPredictBatchOp

Python 类名：KerasSequentialRegressorPredictBatchOp


## 功能介绍

与 KerasSequential 回归训练组件对应的预测组件。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  |  | 256 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
source = CsvSourceBatchOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv") \
    .setSchemaStr("tensor string, label double")

source = ToTensorBatchOp() \
    .setSelectedCol("tensor") \
    .setTensorDataType("DOUBLE") \
    .setTensorShape([200, 3]) \
    .linkFrom(source)

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

predictBatchOp = KerasSequentialRegressorPredictBatchOp() \
    .setPredictionCol("pred") \
    .setReservedCols(["label"]) \
    .linkFrom(trainBatchOp, source)
predictBatchOp.lazyPrint(10)
BatchOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToTensorBatchOp;
import com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorTrainBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class KerasSequentialRegressorTrainBatchOpTest {

	@Test
	public void testKerasSequentialRegressorTrainBatchOp() throws Exception {
		BatchOperator<?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label double");

		source = new ToTensorBatchOp()
			.setSelectedCol("tensor")
			.setTensorDataType("DOUBLE")
			.setTensorShape(200, 3)
			.linkFrom(source);

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

		KerasSequentialRegressorPredictBatchOp predictBatchOp = new KerasSequentialRegressorPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols("label")
			.linkFrom(trainBatchOp, source);
		predictBatchOp.lazyPrint(10);
		BatchOperator.execute();
	}
}
```

### 运行结果

| label  | pred   |
|--------|--------|
| 1.0000 | 0.4822 |
| 0.0000 | 0.4826 |
| 0.0000 | 0.4752 |
| 0.0000 | 0.4702 |
| 1.0000 | 0.4907 |
| 1.0000 | 0.4992 |
| 0.0000 | 0.4866 |
| 1.0000 | 0.5045 |
| 0.0000 | 0.4994 |
| 1.0000 | 0.4837 |
