# Bert文本分类预测 (BertTextClassifierPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.BertTextClassifierPredictBatchOp

Python 类名：BertTextClassifierPredictBatchOp


## 功能介绍

与 BERT 文本分类训练组件对应的预测组件。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  | 256 |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader()
pluginDownloader.downloadPlugin("tf_predictor_macosx") # change according to system type

url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv"
schema = "label bigint, review string";
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schema) \
    .setIgnoreFirstLine(True)
data = data.where("review is not null")
data = data.firstN(300)
model = CsvSourceBatchOp() \
    .setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_classifier_model.csv") \
    .setSchemaStr("model_id bigint, model_info string, label_value bigint")
predict = BertTextClassifierPredictBatchOp() \
    .setPredictionCol("pred") \
    .setPredictionDetailCol("pred_detail") \
    .linkFrom(model, data)
predict.print()
```

### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.BertTextClassifierPredictBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class BertTextClassifierPredictBatchOpTest {

	@Test
	public void testBertTextClassifierPredictBatchOp() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
		pluginDownloader.downloadPlugin("tf_predictor_macosx"); // change according to system type

		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schema = "label bigint, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = data.firstN(300);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_classifier_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value bigint");
		BertTextClassifierPredictBatchOp predict = new BertTextClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.linkFrom(model, data);
		predict.print();
	}
}
```

### 运行结果
|label|review                                           |pred|pred_detail                                    |
|-----|-------------------------------------------------|----|-----------------------------------------------|
|1    |大堂不错，有四星的样子，房间的设施一般，感觉有点旧，卫生间细节不错，各种配套东西都不错，感觉...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |装修较旧,特别是地毯的材质颜色显得较脏,与四星的评级很不相称,门童很热情,赞一个         |1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |住过好多次这家酒店了，上次来到前台，服务员能准确的报出我的名字，感觉很亲切。四星级就是不一样...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |非常不错的酒店，依山傍水，里面大片森林，散散步很不错，坐在湖边也休息也是不错的选择；房间很幽...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |不知怎么回事，一直想给泰安华侨大厦点评，却一直未成功。因为酒店的服务很好。前一段时间和同事一...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|...  |...                                              |... |...                                            |
|1    |还行吧，不算失望，离地铁2号线要走500M左右，但在中环旁边，自己开车去比较方便。        |1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |在携程上订了一天，实际入住两天，酒店照携程价加收了一天房费，并为我免费升级了商务房。中间问酒...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |很不错的酒店。虽然离地铁站有一定距离，可是还是挺方便的。房间的床很大很舒服。但是相对的，别的...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |超赞！虽然窗正对中环，但一点也不觉得吵。房间很干净、整齐，给人很舒服的感觉，服务也很好，价格...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
|1    |7月25日到家人到泰山玩，通过携程订的华侨大厦的房间（说是搞活动，花280升级到360的房间...|1   |{"0":0.2737436294555664,"1":0.7262563705444336}|
