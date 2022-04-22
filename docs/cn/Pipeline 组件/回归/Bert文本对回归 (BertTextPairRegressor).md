# Bert文本对回归 (BertTextPairRegressor)
Java 类名：com.alibaba.alink.pipeline.regression.BertTextPairRegressor

Python 类名：BertTextPairRegressor


## 功能介绍

Bert 文本对回归。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| textCol | 文本列 | 文本列 | String | ✓ |  |  |
| textPairCol | 文本对列 | 文本对列 | String | ✓ |  |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  |  | 32 |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  |  | "Base-Chinese" |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  |  | null |
| customConfigJson | 自定义参数 | 对应 https://github.com/alibaba/EasyTransfer/blob/master/easytransfer/app_zoo/app_config.py 中的config_json | String |  |  |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  |  | 256 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  |  | 0.001 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  |  | 128 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numEpochs | epoch 数 | epoch 数 | Double |  |  | 0.01 |
| numFineTunedLayers | 微调层数 | 微调层数 | Integer |  |  | 1 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv"
schemaStr = "f_quality double, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setFieldDelimiter("\t") \
    .setIgnoreFirstLine(True) \
    .setQuoteChar(None)
data = ShuffleBatchOp().linkFrom(data)

regressor = BertTextPairRegressor() \
    .setTextCol("f_string_1").setTextPairCol("f_string_2").setLabelCol("f_quality") \
    .setNumEpochs(0.1) \
    .setMaxSeqLength(32) \
    .setNumFineTunedLayers(1) \
    .setBertModelName("Base-Uncased") \
    .setPredictionCol("pred")
model = regressor.fit(data)
predict = model.transform(data.firstN(300))
predict.print()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.regression.BertRegressionModel;
import com.alibaba.alink.pipeline.regression.BertTextPairRegressor;
import org.junit.Test;

public class BertTextPairRegressorTest {
	@Test
	public void test() throws Exception {
		String url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv";
		String schemaStr = "f_quality double, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setFieldDelimiter("\t")
			.setIgnoreFirstLine(true)
			.setQuoteChar(null);
		data = new ShuffleBatchOp().linkFrom(data);

		BertTextPairRegressor regressor = new BertTextPairRegressor()
			.setTextCol("f_string_1").setTextPairCol("f_string_2").setLabelCol("f_quality")
			.setNumEpochs(0.1)
			.setMaxSeqLength(32)
			.setNumFineTunedLayers(1)
			.setBertModelName("Base-Uncased")
			.setPredictionCol("pred");
		BertRegressionModel model = regressor.fit(data);
		BatchOperator <?> predict = model.transform(data.firstN(300));
		predict.print();
	}
}
```

### 运行结果

|FIELD1|f_quality|f_id_1  |f_id_2    |f_string_1                                       |f_string_2                                        |
|------|---------|--------|----------|-------------------------------------------------|--------------------------------------------------|
|0     |1.0      |2224622 |2224687   |The indictment ``strikes at one of the very to...|The newly unsealed 32-count indictment alleges... |
|1     |0.0      |58543   |58516     |The tech-heavy Nasdaq Stock Markets composite ...|The Nasdaq Composite index , full of technolog... |
|2     |1.0      |1048622 |1048759   |Rescue workers had to scrounge for boats to re...|Wagner and Wolford both said that rescue worke... |
|3     |1.0      |2733687 |2733747   |Asked about the controversy , Bloomberg said ,...|" I didn 't see either one today , but if they... |
|4     |0.0      |3270421 |3270325   |At 10 p.m. , Odette was centered about 295 mil...|At 10 p.m. Thursday , Odette was about 295 mil... |
|...   |...      |...     |...       |...                                              |...                                               |
|295   |0.0      |2187868 |2187826   |" They were brutally beaten , and it 's really...|" It 's a wonder that Porchia was the only dec... |
|296   |0.0      |257789  |257726    |Knight had 29 interceptions in his six years w...|Knight has 29 interceptions in six seasons , a... |
|297   |1.0      |3128160 |3128278   |It will appear in the next few weeks on the We...|Details of the research will appear in a futur... |
|298   |1.0      |1125898 |1125882   |Following the ATP 's notification by Hewitt 's...|Following ATP 's notification by Hewitt 's law... |
|299   |0.0      |3041807 |3041719   |Details of the research appear in the Nov. 5 i...|The results , published in the Journal of the ... |
