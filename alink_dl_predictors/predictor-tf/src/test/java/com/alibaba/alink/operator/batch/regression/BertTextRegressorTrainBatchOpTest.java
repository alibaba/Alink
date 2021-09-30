package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.regression.BertTextRegressorTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

public class BertTextRegressorTrainBatchOpTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		BatchOperator.setParallelism(1);
		String url = DLTestConstants.CHN_SENTI_CORP_HTL_PATH;
		String schema = "label double, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = new ShuffleBatchOp().linkFrom(data);

		Map <String, Map <String, Object>> customConfig = new HashMap <>();
		customConfig.put("train_config", ImmutableMap.of("optimizer_config", ImmutableMap.of("learning_rate", 0.01)));

		BertTextRegressorTrainBatchOp train = new BertTextRegressorTrainBatchOp()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(0.05)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.setCustomJsonJson(JsonConverter.toJson(customConfig))
			.setModelPath(DLTestConstants.BERT_CHINESE_DIR)
			.setPythonEnv(DLTestConstants.LOCAL_TF115_ENV)
			.linkFrom(data);

		new AkSinkBatchOp()
			.setFilePath("/tmp/bert_text_regressor_model.ak")
			.setOverwriteSink(true)
			.linkFrom(train);

		BatchOperator.execute();
	}
}
