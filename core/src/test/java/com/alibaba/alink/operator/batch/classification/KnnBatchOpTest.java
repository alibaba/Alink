package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class KnnBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		String[] colNames = new String[] {"label", "vector"};
		Row[] trainArray = new Row[] {
			Row.of("f", "2.0,3.0"),
			Row.of("f", "2.1,3.1"),
			Row.of("m", "200.1,300.1"),
			Row.of("m", "200.2,300.2"),
			Row.of("m", "200.3,300.3"),
			Row.of("m", "200.4,300.4"),
			Row.of("m", "200.4,300.4"),
			Row.of("m", "200.6,300.6"),
			Row.of("f", "2.1,3.1"),
			Row.of("f", "2.1,3.1"),
			Row.of("f", "2.1,3.1"),
			Row.of("f", "2.1,3.1"),
			Row.of("f", "2.3,3.2"),
			Row.of("f", "2.3,3.2"),
			Row.of("c", "2.8,3.2"),
			Row.of("d", "300.,3.2"),
			Row.of("f", "2.2,3.2"),
			Row.of("e", "2.4,3.2"),
			Row.of("e", "2.5,3.2"),
			Row.of("e", "2.5,3.2"),
			Row.of("f", "2.1,3.1")
		};

		Row[] testArray =
			new Row[] {
				Row.of("e", "4.0 4.1"),
				Row.of("m", "300 42")
			};

		MemSourceBatchOp trainDataOp = new MemSourceBatchOp(Arrays.asList(trainArray), colNames);
		MemSourceBatchOp testDataOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		BatchOperator <?> knnModel = new KnnTrainBatchOp()
			.setVectorCol("vector")
			.setLabelCol("label")
			.setDistanceType(HasKMeansDistanceType.DistanceType.EUCLIDEAN)
			.linkFrom(trainDataOp);

		KnnPredictBatchOp predictResult = new KnnPredictBatchOp()
			.setPredictionCol("pred")
			.setK(4)
			.setPredictionDetailCol("detail")
			.linkFrom(knnModel, testDataOp);
		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predictResult)
			.collectMetrics();

		Assert.assertTrue(metrics.getAccuracy() > 0.9);

	}
}
