package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.BertTextRegressorPredictBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class BertTextRegressorPredictBatchOpTest {

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
