# Bert文本分类训练 (BertTextClassifierTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.BertTextClassifierTrainBatchOp

Python 类名：BertTextClassifierTrainBatchOp


## 功能介绍

在预训练的 BERT 模型的基础上增加一个全连接层，用于进行文本分类。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| textCol | 文本列 | 文本列 | String | ✓ |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  | 32 |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  | "Base-Chinese" |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  | null |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  | 0.001 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  | 128 |
| numEpochs | epoch 数 | epoch 数 | Double |  | 0.01 |
| numFineTunedLayers | 微调层数 | 微调层数 | Integer |  | 1 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  | null |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。
 如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；
 如果是目录，那么只能使用本地路径，即 file://。 | String |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  | null |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader()

pluginDownloader.downloadPlugin("tf115_python_env_macosx") # change according to system type
pluginDownloader.downloadPlugin("base_chinese_ckpt")

url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv"
schema = "label bigint, review string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schema) \
    .setIgnoreFirstLine(True)
data = data.where("review is not null")

train = BertTextClassifierTrainBatchOp() \
    .setTextCol("review") \
    .setLabelCol("label") \
    .setNumEpochs(2.) \
    .setNumFineTunedLayers(1) \
    .setMaxSeqLength(128) \
    .setBertModelName("Base-Chinese") \
    .linkFrom(data)

AkSinkBatchOp() \
    .setFilePath("/tmp/bert_text_classifier_model.ak") \
    .setOverwriteSink(True) \
    .linkFrom(train)
BatchOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.BertTextClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class BertTextClassifierTrainBatchOpTest {

	@Test
	public void testBertTextClassifierTrainBatchOp() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

		pluginDownloader.downloadPlugin("tf115_python_env_macosx"); // change according to system type
		pluginDownloader.downloadPlugin("base_chinese_ckpt");

		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schema = "label bigint, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");

		BertTextClassifierTrainBatchOp train = new BertTextClassifierTrainBatchOp()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(2.)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.linkFrom(data);

		new AkSinkBatchOp()
			.setFilePath("/tmp/bert_text_classifier_model.ak")
			.setOverwriteSink(true)
			.linkFrom(train);
		BatchOperator.execute();
	}
}
```
