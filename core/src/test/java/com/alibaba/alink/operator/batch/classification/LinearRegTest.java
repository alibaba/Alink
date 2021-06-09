package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LinearRegTest extends AlinkTestBase {

	@Test
	public void batchDenseSparseVectorTest() throws Exception {
		Row[] localRows = new Row[] {
			//Row.of("0:1.0 2:7.0 4:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 2),
			Row.of(0, "1.0 0.0 7.0 0.0 9.0 .0 .0 .0 .0 .0 .0 .0 .0 .0 .0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 2),
			Row.of(1, "0:1.0 2:3.0 4:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 3),
			Row.of(2, "0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1),
			Row.of(3, "0:1.0 2:3.0 14:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 4),
			Row.of(4, "0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 5),
			Row.of(5, "0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 6),
			Row.of(6, "0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 7),
			Row.of(7, "1.0 0.0 2.0 0.0 4.0 .0 .0 .0 .0 .0 .0 .0 .0 .0 .0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1)
		};
		String[] veccolNames = new String[] {"id", "svec", "vec", "f0", "f1", "f2", "label"};

		BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(localRows), veccolNames);
		BatchOperator trainData = vecdata;
		String labelColName = "label";
		LinearRegTrainBatchOp lr = new LinearRegTrainBatchOp()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setOptimMethod("LBFGS")
			.setLabelCol(labelColName)
			.setMaxIter(10);

		LinearRegTrainBatchOp model = lr.linkFrom(trainData);

		List <Row> mixedResult = new LinearRegPredictBatchOp().setReservedCols(new String[] {"id"})
			.setPredictionCol("predLr").setVectorCol("svec")
			.linkFrom(model, trainData).collect();
		for (Row row : mixedResult) {
			if ((int)row.getField(0) == 0) {
				Assert.assertEquals(Double.valueOf(row.getField(1).toString()), 1.9404, 0.001);
			}
		}
	}
}
