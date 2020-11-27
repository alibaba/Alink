package com.alibaba.alink.operator.batch.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorImputerPredictStreamOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorImputer;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class VectorImputerOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = (BatchOperator) com.alibaba.alink.pipeline.dataproc.vector.VectorImputerTest.getData(
			true);
		StreamOperator streamData = (StreamOperator) com.alibaba.alink.pipeline.dataproc.vector.VectorImputerTest
			.getData(false);
		VectorImputerTrainBatchOp op = new VectorImputerTrainBatchOp()
			.setSelectedCol("vec")
			.setStrategy("value")
			.setFillValue(1.0).linkFrom(batchData);
		new VectorImputerPredictBatchOp().setOutputCol("res").linkFrom(op, batchData).lazyCollect();
		new VectorImputerPredictStreamOp(op).setOutputCol("res").linkFrom(streamData).print();

		new VectorImputer()
			.setSelectedCol("vec").setStrategy("mean").fit(batchData).transform(streamData).print();

		new VectorImputer()
			.setSelectedCol("vec").setStrategy("max").fit(batchData).transform(batchData).collect();

		new VectorImputer()
			.setSelectedCol("vec").setStrategy("min").fit(batchData).transform(streamData).print();

		StreamOperator.execute();
	}
}
