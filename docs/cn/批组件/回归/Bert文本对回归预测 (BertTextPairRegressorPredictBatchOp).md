# Bert文本对回归预测 (BertTextPairRegressorPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.BertTextPairRegressorPredictBatchOp

Python 类名：BertTextPairRegressorPredictBatchOp


## 功能介绍

与 BERT 文本对回归训练组件对应的预测组件。


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
url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv"
schemaStr = "f_quality double, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setFieldDelimiter("\t") \
    .setIgnoreFirstLine(True) \
    .setQuoteChar(None)
data = data.firstN(300)
model = CsvSourceBatchOp() \
    .setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_pair_regressor_model.csv") \
    .setSchemaStr("model_id bigint, model_info string, label_value double")
predict = BertTextPairRegressorPredictBatchOp() \
    .setPredictionCol("pred") \
    .setReservedCols(["f_quality"]) \
    .linkFrom(model, data)
predict.print()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.BertTextPairRegressorPredictBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class BertTextPairRegressorPredictBatchOpTest {

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
		data = data.firstN(300);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_pair_regressor_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value double");
		BertTextPairRegressorPredictBatchOp predict = new BertTextPairRegressorPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols("f_quality")
			.linkFrom(model, data);
		predict.print();
	}
}
```

### 运行结果
|f_quality|pred|
|----|--------|
|0.0 |1.404307|
|0.0 |1.404307|
|1.0 |1.404307|
|0.0 |1.404307|
|1.0 |1.404307|
|... |...     |
|0.0 |1.404392|
|1.0 |1.404392|
|0.0 |1.404392|
|1.0 |1.404392|
|1.0 |1.404392|
