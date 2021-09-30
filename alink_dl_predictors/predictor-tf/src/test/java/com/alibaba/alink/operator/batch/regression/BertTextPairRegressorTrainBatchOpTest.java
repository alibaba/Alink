package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.regression.BertTextPairRegressorTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

public class BertTextPairRegressorTrainBatchOpTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		BatchOperator.setParallelism(1);
		String url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv";
		String schemaStr = "f_quality double, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setFieldDelimiter("\t")
			.setIgnoreFirstLine(true)
			.setQuoteChar(null);
		data = new ShuffleBatchOp().linkFrom(data);

		Map <String, Map <String, Object>> customConfig = new HashMap <>();
		customConfig.put("train_config", ImmutableMap.of("optimizer_config", ImmutableMap.of("learning_rate", 0.01)));

		BertTextPairRegressorTrainBatchOp train = new BertTextPairRegressorTrainBatchOp()
			.setTextCol("f_string_1").setTextPairCol("f_string_2").setLabelCol("f_quality")
			.setNumEpochs(0.1)
			.setMaxSeqLength(32)
			.setNumFineTunedLayers(1)
			.setCustomJsonJson(JsonConverter.toJson(customConfig))
			.setBertModelName("Base-Uncased")
			.setModelPath(DLTestConstants.BERT_CHINESE_DIR)
			.setPythonEnv(DLTestConstants.LOCAL_TF115_ENV)
			.linkFrom(data);

		new AkSinkBatchOp()
			.setFilePath("/tmp/bert_text_pair_regressor_model.ak")
			.setOverwriteSink(true)
			.linkFrom(train);
		BatchOperator.execute();
	}
}
