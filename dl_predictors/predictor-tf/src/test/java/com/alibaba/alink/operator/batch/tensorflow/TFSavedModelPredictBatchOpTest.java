package com.alibaba.alink.operator.batch.tensorflow;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFSavedModelPredictBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TFSavedModelPredictBatchOpTest {

	@Category(DLTest.class)
	@Test
	public void testMnist() throws Exception {
		BatchOperator.setParallelism(2);
		String url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/mnist_dense.csv";
		String schema = "label bigint, image string";

		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(url).setSchemaStr(schema).setFieldDelimiter(";");

		BatchOperator <?> predictor = new TFSavedModelPredictBatchOp()
			.setModelPath("http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip")
			.setSelectedCols("image")
			.setOutputSchemaStr("classes bigint, probabilities string");

		data = predictor.linkFrom(data).select("label, classes, probabilities");
		data.print();
	}
}
