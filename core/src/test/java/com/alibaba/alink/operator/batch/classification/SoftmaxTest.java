package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
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
		BatchOperator<?> trainData = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
		String labelColName = "label";
		SoftmaxTrainBatchOp lr = new SoftmaxTrainBatchOp()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setL1(0.0001)
			.setOptimMethod("lbfgs")
			.setLabelCol(labelColName)
			.setMaxIter(20);

		SoftmaxTrainBatchOp model = lr.linkFrom(trainData);

		model.lazyPrintTrainInfo();
		model.lazyPrintModelInfo();

		List <Row> acc = new SoftmaxPredictBatchOp()
			.setPredictionCol("predLr")
			.setPredictionDetailCol("predDetail")
			.linkFrom(model, trainData)
			.link(new EvalMultiClassBatchOp().setLabelCol("predLr").setPredictionDetailCol("predDetail"))
			.link(new JsonValueBatchOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {})
				.setOutputCols(new String[] {"Accuracy"})
				.setJsonPath("$.Accuracy"))
			.collect();
		Assert.assertEquals(Double.parseDouble(acc.get(0).getField(0).toString()), 1.0, 0.001);
	}

	@Test
	public void batchVectorTest() {
		BatchOperator<?> trainData = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
		String labelColName = "label";
		SoftmaxTrainBatchOp lr = new SoftmaxTrainBatchOp()
			.setVectorCol("svec")
			.setStandardization(false)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setOptimMethod("LBFGS")
			.setLabelCol(labelColName)
			.setMaxIter(10);

		SoftmaxTrainBatchOp model = lr.linkFrom(trainData);

		List <Row> acc = new SoftmaxPredictBatchOp()
			.setPredictionCol("predLr").setVectorCol("svec")
			.setPredictionDetailCol("predDetail")
			.linkFrom(model, trainData)
			.link(new EvalMultiClassBatchOp().setLabelCol("predLr").setPredictionDetailCol("predDetail"))
			.link(new JsonValueBatchOp()
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
		BatchOperator<?> batchData = new MemSourceBatchOp(df_data, "f0 int, f1 int, label int");

		BatchOperator<?> softmax = new SoftmaxTrainBatchOp()
			.setMaxIter(8).setFeatureCols("f0", "f1").setStandardization(true).setLabelCol("label");
		BatchOperator<?> model = softmax.linkFrom(batchData);

		BatchOperator<?> finalModel = new SoftmaxTrainBatchOp().setFeatureCols("f0", "f1").setMaxIter(8)
			.setStandardization(false)
			.setLabelCol("label").linkFrom(batchData, model);

		List<Row> result = new SoftmaxPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols("label")
			.linkFrom(finalModel, batchData).collect();

		for (Row row : result) {
			assert (row.getField(0).equals(row.getField(1)));
		}
	}

}
