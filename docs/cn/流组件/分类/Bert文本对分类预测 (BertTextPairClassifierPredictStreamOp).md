# Bert文本对分类预测 (BertTextPairClassifierPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.classification.BertTextPairClassifierPredictStreamOp

Python 类名：BertTextPairClassifierPredictStreamOp


## 功能介绍

与 BERT 文本对分类训练组件对应的预测组件。


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

# If OOM encountered, uncomment the following line and/or use a smaller parallelism
# get_java_class("System").setProperty("direct.reader.policy", "local_file")

url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv"
schemaStr = "f_quality bigint, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string"
data = CsvSourceStreamOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setFieldDelimiter("\t") \
    .setIgnoreFirstLine(True) \
    .setQuoteChar(None)
model = CsvSourceBatchOp() \
    .setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_pair_classifier_model.csv") \
    .setSchemaStr("model_id bigint, model_info string, label_value bigint")
predict = BertTextPairClassifierPredictStreamOp(model) \
    .setPredictionCol("pred") \
    .setPredictionDetailCol("pred_detail") \
    .setReservedCols(["f_quality"]) \
    .linkFrom(data)
predict.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.directreader.DataBridgeGeneratorPolicy;
import com.alibaba.alink.common.io.directreader.LocalFileDataBridgeGenerator;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.BertTextPairClassifierPredictStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class BertTextPairClassifierPredictStreamOpTest {
	@Test
	public void testBertTextPairClassifierPredictStreamOp() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
		pluginDownloader.downloadPlugin("tf_predictor_macosx"); // change according to system type

		StreamOperator.setParallelism(2);	// a larger parallelism needs much more memory

		System.setProperty("direct.reader.policy",
			LocalFileDataBridgeGenerator.class.getAnnotation(DataBridgeGeneratorPolicy.class).policy());

		String url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv";
		String schemaStr = "f_quality bigint, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string";
		StreamOperator <?> data = new CsvSourceStreamOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setFieldDelimiter("\t")
			.setIgnoreFirstLine(true)
			.setQuoteChar(null);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_pair_classifier_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value bigint");
		BertTextPairClassifierPredictStreamOp predict = new BertTextPairClassifierPredictStreamOp(model)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("f_quality")
			.linkFrom(data);
		predict.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f_quality|pred|pred_detail
---------|----|-----------
0|1|{"0":0.072151780128479,"1":0.927848219871521}
1|1|{"0":0.07034707069396973,"1":0.9296529293060303}
0|1|{"0":0.07214611768722534,"1":0.9278538823127747}
0|1|{"0":0.07168549299240112,"1":0.9283145070075989}
1|1|{"0":0.07204830646514893,"1":0.9279516935348511}
...|...|...
