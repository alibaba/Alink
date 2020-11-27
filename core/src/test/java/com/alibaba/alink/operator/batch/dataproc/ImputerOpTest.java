package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.ImputerPredictStreamOp;
import com.alibaba.alink.pipeline.dataproc.Imputer;
import com.alibaba.alink.pipeline.dataproc.ImputerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class ImputerOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = (BatchOperator) com.alibaba.alink.pipeline.dataproc.ImputerTest.getData(true);
		StreamOperator streamData = (StreamOperator) com.alibaba.alink.pipeline.dataproc.ImputerTest.getData(false);
		ImputerModel fillVal = new Imputer()
			.setSelectedCols("f_string").setOutputCols("f_string_2")
			.setStrategy("value")
			.setFillValue("w").fit(batchData);
		fillVal.transform(batchData).lazyCollect();
		fillVal.transform(streamData).print();

		ImputerModel mean = new Imputer()
			.setSelectedCols("f_double", "f_int").setOutputCols("f_double_2", "f_int_2")
			.setStrategy("mean").fit(batchData);
		mean.transform(batchData).lazyCollect();
		mean.transform(streamData).print();

		ImputerModel min = new Imputer()
			.setSelectedCols("f_double")
			.setStrategy("min").fit(batchData);
		min.transform(batchData).lazyCollect();
		min.transform(streamData).print();

		ImputerTrainBatchOp op = new ImputerTrainBatchOp()
			.setSelectedCols("f_double")
			.setStrategy("max")
			.linkFrom(batchData);
		new ImputerPredictBatchOp().linkFrom(op, batchData).collect();
		new ImputerPredictStreamOp(op).linkFrom(streamData).print();

		StreamOperator.execute();
	}

	@Test
	public void testModelInfo() throws Exception {
		BatchOperator batchData = (BatchOperator) com.alibaba.alink.pipeline.dataproc.ImputerTest.getData(true);
		ImputerModel fillVal = new Imputer()
			.setSelectedCols("f_string")
			.setOutputCols("f_string_2")
			.setStrategy("value")
			.setFillValue("w")
			.enableLazyPrintModelInfo()
			.fit(batchData);
		fillVal.transform(batchData).lazyCollect();

		ImputerModel mean = new Imputer()
			.enableLazyPrintModelInfo()
			.setSelectedCols("f_double", "f_int")
			.setOutputCols("f_double_2", "f_int_2")
			.setStrategy("mean").fit(batchData);
		mean.transform(batchData).lazyCollect();

		ImputerModel min = new Imputer()
			.setSelectedCols("f_double")
			.enableLazyPrintModelInfo()
			.setStrategy("min").fit(batchData);
		min.transform(batchData).lazyCollect();

		ImputerTrainBatchOp op = new ImputerTrainBatchOp()
			.setSelectedCols("f_double")
			.setStrategy("max")
			.lazyPrintModelInfo()
			.linkFrom(batchData);
		new ImputerPredictBatchOp().linkFrom(op, batchData).collect();

	}
}
