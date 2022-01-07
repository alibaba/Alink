# Bert文本回归预测 (BertTextRegressorPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.BertTextRegressorPredictStreamOp

Python 类名：BertTextRegressorPredictStreamOp


## 功能介绍

与 BERT 文本回归训练组件对应的预测组件。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  | 256 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
# If OOM encountered, uncomment the following line and/or use a smaller parallelism
# get_java_class("System").setProperty("direct.reader.policy", "local_file")

url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv"
schemaStr = "label bigint, review string"

data = CsvSourceStreamOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setIgnoreFirstLine(True)
model = CsvSourceBatchOp() \
    .setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_regressor_model.csv") \
    .setSchemaStr("model_id bigint, model_info string, label_value double")

predict = BertTextRegressorPredictStreamOp(model) \
    .setPredictionCol("pred") \
    .linkFrom(data)
predict.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.io.directreader.DataBridgeGeneratorPolicy;
import com.alibaba.alink.common.io.directreader.LocalFileDataBridgeGenerator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.BertTextRegressorPredictStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class BertTextRegressorPredictStreamOpTest {

	@Test
	public void testBertTextRegressorPredictStreamOp() throws Exception {
		StreamOperator.setParallelism(2);	// a larger parallelism needs much more memory

		System.setProperty("direct.reader.policy",
			LocalFileDataBridgeGenerator.class.getAnnotation(DataBridgeGeneratorPolicy.class).policy());
		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schemaStr = "label double, review string";

		StreamOperator <?> data = new CsvSourceStreamOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setIgnoreFirstLine(true);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_regressor_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value double");

		BertTextRegressorPredictStreamOp predict = new BertTextRegressorPredictStreamOp(model)
			.setPredictionCol("pred")
			.linkFrom(data);
		predict.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

label|review|pred
-----|------|----
1.0000|距离川沙公路较近,但是公交指示不对,如果是"蔡陆线"的话,会非常麻烦.建议用别的路线.房间较为简单.|5.0041
1.0000|大堂不错，有四星的样子，房间的设施一般，感觉有点旧，卫生间细节不错，各种配套东西都不错，感觉还可以，有机会再去泰山还要入住。|5.2454
1.0000|商务大床房，房间很大，床有2M宽，整体感觉经济实惠不错!|5.6348
1.0000|装修较旧,特别是地毯的材质颜色显得较脏,与四星的评级很不相称,门童很热情,赞一个|3.0199
1.0000|住过好多次这家酒店了，上次来到前台，服务员能准确的报出我的名字，感觉很亲切。四星级就是不一样。而且当天服务员还给我安排了一间商务单间，房间很新，比我订的要好价格没变。说是酒店搞活动，像我们这样的商务客人都有机会享受，不错。|6.7312
...|...|...
