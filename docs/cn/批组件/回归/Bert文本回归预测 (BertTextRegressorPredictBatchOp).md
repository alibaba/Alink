# Bert文本回归预测 (BertTextRegressorPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.BertTextRegressorPredictBatchOp

Python 类名：BertTextRegressorPredictBatchOp


## 功能介绍

与 BERT 文本回归训练组件对应的预测组件。


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
url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv"
schema = "label double, review string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schema) \
    .setIgnoreFirstLine(True)
data = data.where("review is not null")
data = data.firstN(300)
model = CsvSourceBatchOp() \
    .setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_regressor_model.csv") \
    .setSchemaStr("model_id bigint, model_info string, label_value double")
predict = BertTextRegressorPredictBatchOp() \
    .setPredictionCol("pred") \
    .setReservedCols(["label"]) \
    .linkFrom(model, data)
predict.print()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.BertTextRegressorPredictBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class BertTextRegressorPredictBatchOpTest {

	@Test
	public void testBertTextRegressorPredictBatchOp() throws Exception {
		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schema = "label double, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = data.firstN(300);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_regressor_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value double");
		BertTextRegressorPredictBatchOp predict = new BertTextRegressorPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols("label")
			.linkFrom(model, data);
		predict.print();
	}
}
```

### 运行结果
|label|   pred|
|----|--------|
|1.0 |5.004022|
|1.0 |5.004022|
|1.0 |5.004022|
|1.0 |5.004022|
|1.0 |5.004022|
|... |...     |
|0.0 |5.004022|
|0.0 |5.004022|
|0.0 |5.004022|
|0.0 |5.004022|
|0.0 |5.004022|
