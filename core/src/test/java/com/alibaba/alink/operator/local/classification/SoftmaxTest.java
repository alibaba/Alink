package com.alibaba.alink.operator.local.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.JsonValueLocalOp;
import com.alibaba.alink.operator.local.evaluation.EvalMultiClassLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SoftmaxTest extends AlinkTestBase {

	Row[] vecrows = new Row[] {
		Row.of("0:1.0 2:7.0 4:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 2),
		Row.of("0:1.0 2:3.0 4:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 3),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1),
		Row.of("0:1.0 2:3.0 14:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 4),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 5),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 6),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 7),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1)
	};
	String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};

	@Test
	public void batchTableTest() {
		LocalOperator <?> trainData = new MemSourceLocalOp(Arrays.asList(vecrows), veccolNames);
		String labelColName = "label";
		SoftmaxTrainLocalOp lr = new SoftmaxTrainLocalOp()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setL1(0.0001)
			.setOptimMethod("lbfgs")
			.setLabelCol(labelColName)
			.setMaxIter(20);

		SoftmaxTrainLocalOp model = lr.linkFrom(trainData);

		model.lazyPrintTrainInfo();
		model.lazyPrintModelInfo();

		List <Row> acc = new SoftmaxPredictLocalOp()
			.setPredictionCol("predLr")
			.setPredictionDetailCol("predDetail")
			.linkFrom(model, trainData)
			.link(new EvalMultiClassLocalOp().setLabelCol("predLr").setPredictionDetailCol("predDetail"))
			.link(new JsonValueLocalOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {})
				.setOutputCols(new String[] {"Accuracy"})
				.setJsonPath("$.Accuracy"))
			.collect();
		Assert.assertEquals(Double.parseDouble(acc.get(0).getField(0).toString()), 1.0, 0.001);
	}

	@Test
	public void batchVectorTest() {
		LocalOperator<?> trainData = new MemSourceLocalOp(Arrays.asList(vecrows), veccolNames);
		String labelColName = "label";
		SoftmaxTrainLocalOp lr = new SoftmaxTrainLocalOp()
			.setVectorCol("svec")
			.setStandardization(false)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setOptimMethod("LBFGS")
			.setLabelCol(labelColName)
			.setMaxIter(10);

		SoftmaxTrainLocalOp model = lr.linkFrom(trainData);

		List <Row> acc = new SoftmaxPredictLocalOp()
			.setPredictionCol("predLr").setVectorCol("svec")
			.setPredictionDetailCol("predDetail")
			.linkFrom(model, trainData)
			.link(new EvalMultiClassLocalOp().setLabelCol("predLr").setPredictionDetailCol("predDetail"))
			.link(new JsonValueLocalOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {})
				.setOutputCols(new String[] {"Accuracy"})
				.setJsonPath("$.Accuracy"))
			.collect();
		Assert.assertEquals(Double.parseDouble(acc.get(0).getField(0).toString()), 1.0, 0.001);
	}

	@Test
	public void testIncrementalSoftmax() {
		List <Row> df_data = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 3)
		);
		LocalOperator<?> batchData = new MemSourceLocalOp(df_data, "f0 int, f1 int, label int");

		LocalOperator<?> softmax = new SoftmaxTrainLocalOp()
			.setMaxIter(8).setFeatureCols("f0", "f1").setStandardization(true).setLabelCol("label");
		LocalOperator<?> model = softmax.linkFrom(batchData);

		LocalOperator<?> finalModel = new SoftmaxTrainLocalOp().setFeatureCols("f0", "f1").setMaxIter(8)
			.setStandardization(false)
			.setLabelCol("label").linkFrom(batchData, model);

		List<Row> result = new SoftmaxPredictLocalOp()
			.setPredictionCol("pred")
			.setReservedCols("label")
			.linkFrom(finalModel, batchData).collect();

		for (Row row : result) {
			assert (row.getField(0).equals(row.getField(1)));
		}
	}

}
