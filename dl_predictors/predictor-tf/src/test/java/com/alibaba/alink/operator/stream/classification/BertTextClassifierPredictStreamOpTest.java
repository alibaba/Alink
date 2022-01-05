package com.alibaba.alink.operator.stream.classification;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.directreader.DataBridgeGeneratorPolicy;
import com.alibaba.alink.common.io.directreader.LocalFileDataBridgeGenerator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class BertTextClassifierPredictStreamOpTest {

	@Test
	public void test() throws Exception {
		System.setProperty("direct.reader.policy",
			LocalFileDataBridgeGenerator.class.getAnnotation(DataBridgeGeneratorPolicy.class).policy());
		int savedBatchParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		int savedStreamParallelism = MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(1);
		StreamOperator.setParallelism(1);
		String url = DLTestConstants.CHN_SENTI_CORP_HTL_PATH;
		String schemaStr = "label bigint, review string";

		StreamOperator <?> data = new CsvSourceStreamOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setIgnoreFirstLine(true);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_classifier_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value bigint");

		BertTextClassifierPredictStreamOp predict = new BertTextClassifierPredictStreamOp(model)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.linkFrom(data);
		predict.print();
		StreamOperator.execute();
		BatchOperator.setParallelism(savedBatchParallelism);
		StreamOperator.setParallelism(savedStreamParallelism);
	}
}
