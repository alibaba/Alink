# Bert文本回归 (BertTextRegressor)
Java 类名：com.alibaba.alink.pipeline.regression.BertTextRegressor

Python 类名：BertTextRegressor


## 功能介绍

Bert 文本回归。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| textCol | 文本列 | 文本列 | String | ✓ |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  | 32 |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  | "Base-Chinese" |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  | null |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  | 256 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  | 0.001 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  | 128 |
| numEpochs | epoch 数 | epoch 数 | Double |  | 0.01 |
| numFineTunedLayers | 微调层数 | 微调层数 | Integer |  | 1 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
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
pluginDownloader.downloadPlugin("tf115_python_env_macosx") # change according to system type
pluginDownloader.downloadPlugin("base_chinese_ckpt")
pluginDownloader.downloadPlugin("tf_predictor_macosx") # change according to system type

url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv"
schemaStr = "label double, review string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setIgnoreFirstLine(True)
data = data.where("review is not null")
data = ShuffleBatchOp().linkFrom(data)

regressor = BertTextRegressor() \
    .setTextCol("review") \
    .setLabelCol("label") \
    .setNumEpochs(0.01) \
    .setNumFineTunedLayers(1) \
    .setMaxSeqLength(128) \
    .setBertModelName("Base-Chinese") \
    .setPredictionCol("pred")
model = regressor.fit(data)
predict = model.transform(data.firstN(300))
predict.print()
```

### Java 代码
```java
import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.regression.BertRegressionModel;
import com.alibaba.alink.pipeline.regression.BertTextRegressor;
import org.junit.Test;

public class BertTextRegressorTest {
	@Test
	public void test() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
		pluginDownloader.downloadPlugin("tf115_python_env_macosx"); // change according to system type
		pluginDownloader.downloadPlugin("base_chinese_ckpt");
		pluginDownloader.downloadPlugin("tf_predictor_macosx"); // change according to system type

		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schemaStr = "label double, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = new ShuffleBatchOp().linkFrom(data);

		BertTextRegressor regressor = new BertTextRegressor()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(0.01)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.setPredictionCol("pred");
		BertRegressionModel model = regressor.fit(data);
		BatchOperator <?> predict = model.transform(data.firstN(300));
		predict.print();
	}
}
```

### 运行结果

|      |label|review  |pred      |
|------|-----|--------|----------|
|0     |0.0  |出差入住的酒店,订了个三人间.房间没空调,冷得要死,而且被子很潮.火车站旁,步行可到.据说当...|0.711733  |
|1     |1.0  |我已经住过两次了。比较满意，特别是接机服务，一个人也接，及时周到。宽带速度也比较快。但是没有...|0.861643  |
|2     |1.0  |酒店的设施稍微老了一点，是四星级酒店的一个附楼。但是服务不错，而且酒店的价格非常吸引人。下次...|0.933909  |
|3     |1.0  |价格比比较不错的酒店。这次免费升级了，感谢前台服务员。房子还好，地毯是新的，比上次的好些。早...|0.863195  |
|4     |1.0  |前台小姑娘很有意思，经常入住，都熟了，呵呵|0.521080  |
|...   |...  |...     |...       |
|113   |1.0  |1。酒店比较新，装潢和设施还不错，只是房间有些油漆味。2。早餐还可以，只是品种不是很多。3。...|0.710072  |
|114   |1.0  |这次去北京，是要去北师大办事，所以特意留意了下附近的宾馆。住了两天，首先该宾馆很好找，离西四...|1.596990  |
|115   |0.0  |1。我住的是靠马路的标准间。房间内设施简陋，并且的房间玻璃窗户外还有一层幕墙玻璃，而且不能打...|1.823668  |
|116   |1.0  |房间很清洁，洗手间装修好。早餐非常丰盛，豆腐脑很好吃！交通很方便。隔音不够好，我们的邻居夜里...|0.972452  |
|117   |1.0  |价格偏高,好象连云港这地方的酒店都偏贵.早饭不好.房间还不错,窗外风景还行.最重要是房间的窗...|0.744328  |
