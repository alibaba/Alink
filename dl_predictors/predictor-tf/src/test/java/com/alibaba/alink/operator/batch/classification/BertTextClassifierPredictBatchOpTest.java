package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class BertTextClassifierPredictBatchOpTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(2);
		String url = DLTestConstants.CHN_SENTI_CORP_HTL_PATH;
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
		BatchOperator.setParallelism(savedParallelism);
	}
}
