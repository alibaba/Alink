package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.fm.FmPredictBatchOp;
import com.alibaba.alink.pipeline.classification.FmClassifier;
import com.alibaba.alink.pipeline.classification.FmModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FmClassifierTest extends AlinkTestBase {
	@Test
	public void testFm() {
		BatchOperator <?> trainData = new MemSourceBatchOp(
			new Object[][] {
				{"1.1 2.0", 1.0},
				{"2.1 3.1", 1.0},
				{"3.1 2.2", 1.0},
				{"1.2 3.2", 0.0},
				{"1.2 4.2", 0.0}
			},
			new String[] {"vec", "label"});
		FmClassifierTrainBatchOp adagrad = new FmClassifierTrainBatchOp()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setNumEpochs(10)
			.setInitStdev(0.01)
			.setLearnRate(0.01)
			.setEpsilon(0.0001)
			.linkFrom(trainData);
		adagrad.lazyPrintModelInfo();
		adagrad.lazyPrintTrainInfo();
		BatchOperator <?> result = new FmPredictBatchOp().setVectorCol("vec").setPredictionCol("pred")
			.setPredictionDetailCol("details")
			.linkFrom(adagrad, trainData);

		List <Row> eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("details")
			.linkFrom(result)
			.link(new JsonValueBatchOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {"Statistics"})
				.setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
				.setJsonPath("$.Accuracy", "$.AUC", "$.ConfusionMatrix"))
			.collect();
		Assert.assertEquals(Double.parseDouble(eval.get(0).getField(0).toString()), 0.6, 0.01);
	}

	@Test
	public void testFmSparse() {
		BatchOperator <?> trainData = new MemSourceBatchOp(
			new Object[][] {
				{"1:1.1 3:2.0", 1.0},
				{"2:2.1 10:3.1", 1.0},
				{"3:3.1 7:2.2", 1.0},
				{"1:1.2 5:3.2", 0.0},
				{"3:1.2 7:4.2", 0.0}
			},
			new String[] {"vec", "label"});
		FmClassifierTrainBatchOp adagrad = new FmClassifierTrainBatchOp()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setNumEpochs(10)
			.setInitStdev(0.01)
			.setLearnRate(0.01)
			.setEpsilon(0.0001)
			.linkFrom(trainData);

		BatchOperator <?> result = new FmPredictBatchOp().setVectorCol("vec").setPredictionCol("pred")
			.setPredictionDetailCol("details")
			.linkFrom(adagrad, trainData);

		List <Row> eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("details")
			.linkFrom(result)
			.link(new JsonValueBatchOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {"Statistics"})
				.setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
				.setJsonPath("$.Accuracy", "$.AUC", "$.ConfusionMatrix"))
			.collect();
		Assert.assertEquals(Double.parseDouble(eval.get(0).getField(0).toString()), 0.8, 0.01);
	}

	@Test
	public void testPipelineFmSparse() {
		BatchOperator<?> trainData = new MemSourceBatchOp(
			new Object[][] {
				{"0:1.1 1:2.0", 1.0},
				{"0:2.1 1:3.1", 1.0},
				{"0:3.1 1:2.2", 1.0},
				{"0:5.2 1:3.2", 0.0},
				{"0:5.2 1:4.2", 0.0}
			},
			new String[] {"vec", "label"});
		FmClassifier adagrad = new FmClassifier()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setNumEpochs(10)
			.setInitStdev(0.01)
			.setLearnRate(0.01)
			.setEpsilon(0.0001)
			.setPredictionCol("pred")
			.enableLazyPrintModelInfo();

		FmModel<?> model = adagrad.fit(trainData);
		BatchOperator<?> result = model.transform(trainData).lazyPrint(10);
		List<Row> rows = result.collect();
		for (Row row : rows) {
			Assert.assertEquals(row.getField(1), row.getField(2));
		}
	}
}
