package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.BertTextPairClassifierPredictBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class BertTextPairClassifierPredictBatchOpTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		BatchOperator.setParallelism(1);
		String url = "http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/data/MRPC/train.tsv";
		String schemaStr = "f_quality bigint, f_id_1 string, f_id_2 string, f_string_1 string, f_string_2 string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setFieldDelimiter("\t")
			.setIgnoreFirstLine(true)
			.setQuoteChar(null);
		data = data.firstN(300);
		BatchOperator <?> model = new CsvSourceBatchOp()
			.setFilePath("http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert_text_pair_classifier_model.csv")
			.setSchemaStr("model_id bigint, model_info string, label_value bigint");
		BertTextPairClassifierPredictBatchOp predict = new BertTextPairClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("f_quality")
			.linkFrom(model, data);
		predict.print();
	}
}
