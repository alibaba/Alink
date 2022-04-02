package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.LogisticRegressionPredictStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class LogisticRegressionTest extends AlinkTestBase {

	AlgoOperator<?> getData(boolean isBatch) {
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

		if (isBatch) {
			return new MemSourceBatchOp(
				Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
		} else {
			return new MemSourceStreamOp(
				Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
		}
	}

	@Test
	public void batchTest() throws Exception {
		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";
		String vectorName = "vec";
		String svectorName = "svec";
		BatchOperator<?> trainData = (BatchOperator) getData(true);

		LogisticRegressionTrainBatchOp svm = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(false)
			.setFeatureCols(xVars)
			.setOptimMethod("lbfgs").linkFrom(trainData);

		LogisticRegressionTrainBatchOp vectorSvm = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(false)
			.setVectorCol(vectorName).linkFrom(trainData);

		LogisticRegressionTrainBatchOp sparseVectorSvm = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setVectorCol(svectorName)
			.setWithIntercept(false)
			.setStandardization(false)
			.setOptimMethod("newton")
			.setMaxIter(10).linkFrom(trainData);

		BatchOperator<?> result1 = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("lrpred").linkFrom(svm, trainData);

		BatchOperator<?> result2 = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("svpred").linkFrom(vectorSvm, result1);
		BatchOperator<?> result3 = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("dvpred").linkFrom(sparseVectorSvm, result2);

		result3.lazyCollect(new Consumer <List <Row>>() {
			@Override
			public void accept(List <Row> d) {
				for (Row row : d) {
					for (int i = 7; i < 10; ++i) {
						Assert.assertEquals(row.getField(6), row.getField(i));
					}
				}
			}
		});
		result3.collect();
	}


	@Test
	public void streamTest() throws Exception {
		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";
		String vectorName = "vec";
		String svectorName = "svec";
		BatchOperator<?> trainData = (BatchOperator<?>) getData(true);

		LogisticRegressionTrainBatchOp svm = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(false)
			.setFeatureCols(xVars)
			.setOptimMethod("lbfgs").linkFrom(trainData);

		LogisticRegressionTrainBatchOp vectorSvm = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(false)
			.setVectorCol(vectorName).linkFrom(trainData);

		LogisticRegressionTrainBatchOp sparseVectorSvm = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setVectorCol(svectorName)
			.setWithIntercept(false)
			.setStandardization(false)
			.setOptimMethod("newton")
			.setMaxIter(10).linkFrom(trainData);

		StreamOperator <?> result1 = new LogisticRegressionPredictStreamOp(svm)
			.setPredictionCol("lrpred").linkFrom((StreamOperator<?>) getData(false));

		StreamOperator <?> result2 = new LogisticRegressionPredictStreamOp(vectorSvm)
			.setPredictionCol("svpred").linkFrom(result1);
		StreamOperator <?> result3 = new LogisticRegressionPredictStreamOp(sparseVectorSvm)
			.setPredictionCol("dvpred").linkFrom(result2);

		 CollectSinkStreamOp sop = result3.link(new CollectSinkStreamOp());
		 StreamOperator.execute();
		 List<Row> rows = sop.getAndRemoveValues();

		for (Row row : rows) {
			for (int i = 7; i < 10; ++i) {
				Assert.assertEquals(row.getField(6), row.getField(i));
			}
		}
	}

	@Test
	public void incrementalTest() throws Exception {
		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";

		BatchOperator<?> trainData = (BatchOperator<?>) getData(true);

		LogisticRegressionTrainBatchOp initModel = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(true)
			.setFeatureCols(xVars)
			.setMaxIter(5)
			.setOptimMethod("lbfgs").linkFrom(trainData);

	    BatchOperator<?> finalModel = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(true)
			.setMaxIter(5)
			.setFeatureCols(xVars)
			.setOptimMethod("lbfgs").linkFrom(trainData, initModel);

		List<Row> result = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols(yVar)
			.linkFrom(finalModel, trainData).collect();
		for (Row row : result) {
			assert (row.getField(0).equals(row.getField(1)));
		}
	}

	@Test
	public void incrementalVectorTest() {
		String yVar = "labels";

		BatchOperator<?> trainData = (BatchOperator<?>) getData(true);

		LogisticRegressionTrainBatchOp initModel = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(true)
			.setVectorCol("vec")
			.setMaxIter(5)
			.setOptimMethod("lbfgs").linkFrom(trainData);

		BatchOperator<?> finalModel = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(true)
			.setMaxIter(5)
			.setVectorCol("vec")
			.setOptimMethod("lbfgs").linkFrom(trainData, initModel);

		List<Row> result = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols(yVar)
			.linkFrom(finalModel, trainData).collect();
		for (Row row : result) {
			assert (row.getField(0).equals(row.getField(1)));
		}
	}

	@Test
	public void incrementalSVectorTest() throws Exception {
		String yVar = "labels";

		BatchOperator<?> trainData = (BatchOperator<?>) getData(true);

		LogisticRegressionTrainBatchOp initModel = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(true)
			.setVectorCol("svec")
			.setMaxIter(5)
			.setOptimMethod("lbfgs").linkFrom(trainData);

		BatchOperator<?> finalModel = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setWithIntercept(false)
			.setStandardization(false)
			.setMaxIter(5)
			.setVectorCol("svec")
			.setOptimMethod("lbfgs").linkFrom(trainData, initModel);

		List<Row> result = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("pred")
			.setReservedCols(yVar)
			.linkFrom(finalModel, trainData).collect();
		for (Row row : result) {
			assert (row.getField(0).equals(row.getField(1)));
		}
	}
}
