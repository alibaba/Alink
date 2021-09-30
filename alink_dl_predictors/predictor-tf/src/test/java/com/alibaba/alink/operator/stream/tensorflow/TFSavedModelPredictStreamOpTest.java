package com.alibaba.alink.operator.stream.tensorflow;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.tensorflow.TFSavedModelPredictStreamOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TFSavedModelPredictStreamOpTest {

	@Category(DLTest.class)
	@Test
	public void testMnist() throws Exception {
		StreamOperator.setParallelism(2);
		String url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/mnist_dense.csv";
		String schema = "label bigint, image string";

		StreamOperator <?> data = new CsvSourceStreamOp().setFilePath(url).setSchemaStr(schema).setFieldDelimiter(";");

		StreamOperator <?> predictor = new TFSavedModelPredictStreamOp()
			.setModelPath("http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip")
			.setSelectedCols("image")
			.setOutputSchemaStr("classes bigint, probabilities string");

		data = predictor.linkFrom(data).select("label, classes, probabilities");
		data.print();
		StreamOperator.execute();
	}
}
