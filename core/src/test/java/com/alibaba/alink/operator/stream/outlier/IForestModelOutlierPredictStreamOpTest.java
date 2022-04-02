package com.alibaba.alink.operator.stream.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IForestModelOutlierTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class IForestModelOutlierPredictStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		Object[][] dataArrays = new Object[][] {
			{0.73, 0},
			{0.24, 0},
			{0.63, 0},
			{0.55, 0},
			{0.73, 0},
			{0.41, 0}
		};

		String[] colNames = new String[] {"val", "label"};

		BatchOperator <?> data = new MemSourceBatchOp(dataArrays, colNames);

		StreamOperator <?> streamData = new MemSourceStreamOp(dataArrays, colNames);

		IForestModelOutlierTrainBatchOp trainOp = new IForestModelOutlierTrainBatchOp()
			.setFeatureCols("val");

		IForestModelOutlierPredictStreamOp predOp = new IForestModelOutlierPredictStreamOp(trainOp.linkFrom(data))
			.setOutlierThreshold(3.0)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		predOp.linkFrom(streamData).print();

		StreamOperator.execute();

	}
}