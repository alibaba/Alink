# Bert文本对分类器 (BertTextPairClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.BertTextPairClassifier

Python 类名：BertTextPairClassifier


## 功能介绍

Bert 文本对分类器。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| textCol | 文本列 | 文本列 | String | ✓ |  |
| textPairCol | 文本对列 | 文本对列 | String | ✓ |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  | 32 |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  | "Base-Chinese" |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  | null |
| customConfigJson | 自定义参数 | 对应 https://github.com/alibaba/EasyTransfer/blob/master/easytransfer/app_zoo/app_config.py 中的config_json | String |  |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  | 256 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  | 0.001 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  | 128 |
| numEpochs | epoch 数 | epoch 数 | Double |  | 0.01 |
| numFineTunedLayers | 微调层数 | 微调层数 | Integer |  | 1 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv"
schemaStr = "f_quality bigint, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setFieldDelimiter("\t") \
    .setIgnoreFirstLine(True) \
    .setQuoteChar(None)
data = ShuffleBatchOp().linkFrom(data)

classifier = BertTextPairClassifier() \
    .setTextCol("f_string_1").setTextPairCol("f_string_2").setLabelCol("f_quality") \
    .setNumEpochs(0.1) \
    .setMaxSeqLength(32) \
    .setNumFineTunedLayers(1) \
    .setBertModelName("Base-Uncased") \
    .setPredictionCol("pred") \
    .setPredictionDetailCol("pred_detail")
model = classifier.fit(data)
predict = model.transform(data.firstN(300))
predict.print()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.BertClassificationModel;
import com.alibaba.alink.pipeline.classification.BertTextClassifier;
import org.junit.Test;

public class BertTextClassifierTest {
	@Test
	public void test() throws Exception {
		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schemaStr = "label bigint, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = new ShuffleBatchOp().linkFrom(data);

		BertTextClassifier classifier = new BertTextClassifier()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(0.01)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");
		BertClassificationModel model = classifier.fit(data);
		BatchOperator <?> predict = model.transform(data.firstN(300));
		predict.print();
	}
}
```

### 运行结果

|f_quality|f_id_1  |f_id_2    |f_string_1                                                                                            |f_string_2                                       |pred|pred_detail                                     |
|---------|--------|----------|------------------------------------------------------------------------------------------------------|-------------------------------------------------|----|------------------------------------------------|
|0        |218017  |218035    |Application Intelligence will be included as p...                                                     |The new application intelligence features will...|1   |{"0":0.20335173606872559,"1":0.7966482639312744}|
|1        |1642169 |1642368   |The new 25-member Governing Council 's first m...                                                     |Its first decisions were to scrap all holidays...|1   |{"0":0.20335173606872559,"1":0.7966482639312744}|
|1        |3399091 |3399055   |Also in Mosul , rebel gunmen on Friday assassi...                                                     |Near a mosque in the northern town of Mosul , ...|1   |{"0":0.20335173606872559,"1":0.7966482639312744}|
|0        |2583299 |2583319   |" We 're still confident that Gephardt will ge...	                                                     |Whether or not we get to the two-thirds , we '...|1	|{"0":0.20335173606872559,"1":0.7966482639312744}|
|1        |1568540 |1568627   |Monday , the CIA said analysts concluded that ...                                                     |The CIA on Monday said voice and sound analyst...|1   |{"0":0.20335173606872559,"1":0.7966482639312744}|
|...      |...     |...       |...                                                                                                   |...                                              |... |...                                             |
|1        |1805639 |1805436   |Printer maker Lexmark International Inc. spurt...                                                     |Other gainers included Lexmark , which rose $ ...|1   |{"0":0.23678696155548096,"1":0.763213038444519} |
|1        |2182211 |2182122   |It ended a diplomatic drought between the two ...                                                     |The contact between the delegations ended a di...|1   |{"0":0.23678696155548096,"1":0.763213038444519} |
|1        |774666  |774871    |An unclear number of people were killed and mo...                                                     |Four people were killed and 50 injured in the ...|1   |{"0":0.23678696155548096,"1":0.763213038444519} |
|0        |2582380 |2582198   |Shaklee spokeswoman Jenifer Thompson said the ...                                                     |Shaklee spokeswoman Jenifer Thompson referred ...|1   |{"0":0.23678696155548096,"1":0.763213038444519} |
|1        |427232  |427141    |After three months , Atkins dieters had lost a...                                                     |Three months into the study , the Atkins group...|1   |{"0":0.23678696155548096,"1":0.763213038444519} |
