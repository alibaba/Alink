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
 * Test cases for svm.
 */
public class SvmTest {

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
		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";
		String vectorName = "vec";
		String svectorName = "svec";
		BatchOperator trainData = (BatchOperator) getData(true);

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

		BatchOperator result1 = new LinearSvmPredictBatchOp()
			.setPredictionCol("svmpred").linkFrom(svm, trainData);
		BatchOperator result2 = new LinearSvmPredictBatchOp()
			.setPredictionCol("svsvmpred").linkFrom(vectorSvm, result1);
		BatchOperator result3 = new LinearSvmPredictBatchOp()
			.setReservedCols(new String[]{yVar,"svmpred", "svsvmpred"})
			.setPredictionCol("dvsvmpred").linkFrom(sparseVectorSvm, result2);

		result3.lazyCollect(new Consumer<List<Row>>() {
			@Override
			public void accept(List<Row> d) {
				for (Row row : d) {
					for (int i = 1; i < 4; ++i) {
						Assert.assertEquals(row.getField(0), row.getField(i));
					}
				}
			}
		});
		result3.collect();
	}
}
