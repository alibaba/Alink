package com.alibaba.alink.operator.batch.classification;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for lr.
 */
public class LogisticRegressionTest {

	AlgoOperator getData(boolean isBatch) {
		Row[] array = new Row[] {
			Row.of(new Object[] {"$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1}),
			Row.of(new Object[] {"$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1}),
			Row.of(new Object[] {"$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1}),
			Row.of(new Object[] {"$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1}),
			Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0}),
			Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0}),
			Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0}),
			Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0})
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
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";
		String vectorName = "vec";
		String svectorName = "svec";
		BatchOperator trainData = (BatchOperator) getData(true);

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

		BatchOperator result1 = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("lrpred").linkFrom(svm, trainData);

		BatchOperator result2 = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("svpred").linkFrom(vectorSvm, result1);
		BatchOperator result3 = new LogisticRegressionPredictBatchOp()
			.setPredictionCol("dvpred").linkFrom(sparseVectorSvm, result2);

		result3.lazyCollect(new Consumer<List<Row>>() {
			@Override
			public void accept(List<Row> d) {
				for (Row row : d) {
					for (int i = 7; i < 10; ++i) {
						Assert.assertEquals(row.getField(6), row.getField(i));
					}
				}
			}
		});
		result3.collect();
	}
}
