package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SvmTest extends AlinkTestBase {

	BatchOperator <?> getData() {
		Row[] array = new Row[] {
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};

		return new MemSourceBatchOp(
			Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});

	}

	@Test
	public void batchTest() {
		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";
		String vectorName = "vec";
		String svectorName = "svec";
		BatchOperator <?> trainData = getData();

		LinearSvmTrainBatchOp svm = new LinearSvmTrainBatchOp()
			.setLabelCol(yVar)
			.setFeatureCols(xVars)
			.setOptimMethod("gd").linkFrom(trainData);

		svm.lazyPrintTrainInfo();
		svm.lazyPrintModelInfo();
		svm.getSideOutput(0).collect();
		svm.getSideOutput(1).collect();

		LinearSvmTrainBatchOp vectorSvm = new LinearSvmTrainBatchOp()
			.setLabelCol(yVar)
			.setVectorCol(vectorName).linkFrom(trainData);

		LinearSvmTrainBatchOp sparseVectorSvm = new LinearSvmTrainBatchOp()
			.setLabelCol(yVar)
			.setVectorCol(svectorName)
			.setOptimMethod("lbfgs")
			.setMaxIter(100).linkFrom(trainData);

		BatchOperator <?> result1 = new LinearSvmPredictBatchOp()
			.setPredictionCol("svmpred").linkFrom(svm, trainData);
		BatchOperator <?> result2 = new LinearSvmPredictBatchOp()
			.setPredictionCol("svsvmpred").linkFrom(vectorSvm, result1);
		BatchOperator <?> result3 = new LinearSvmPredictBatchOp()
			.setReservedCols(new String[] {yVar, "svmpred", "svsvmpred"})
			.setPredictionCol("dvsvmpred").linkFrom(sparseVectorSvm, result2);

		List <Row> d = result3.collect();
		for (Row row : d) {
			for (int i = 1; i < 4; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}
	}
}
