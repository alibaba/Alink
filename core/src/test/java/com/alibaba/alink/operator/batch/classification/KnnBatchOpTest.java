package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.VectorToColumnsBatchOp;
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
	public void test1() throws Exception {
		String labelName = "g";

		Row[] testArray = new Row[] {
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

		String[] colnames = new String[] {"g", "vector"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		Row[] preArray =
			new Row[] {
				Row.of("e", "4.0 4.1"),
				Row.of("m", "300 42")
			};

		MemSourceBatchOp outOp = new MemSourceBatchOp(Arrays.asList(preArray), new String[] {"g", "vector"});

		VectorToColumnsBatchOp vectorToColumnsBatchOp = new VectorToColumnsBatchOp()
			.setVectorCol("vector")
			.setSchemaStr("f0 double, f1 double")
			.linkFrom(inOp);

		BatchOperator<?> op = new KnnTrainBatchOp()
			.setFeatureCols("f0", "f1")
			.setLabelCol(labelName)
			.setDistanceType(HasKMeansDistanceType.DistanceType.EUCLIDEAN)
			.linkFrom(vectorToColumnsBatchOp);

		KnnPredictBatchOp predict = new KnnPredictBatchOp()
			.setPredictionCol("pred")
			.setK(4)
			.setPredictionDetailCol("detail")
			.linkFrom(op, vectorToColumnsBatchOp.linkFrom(outOp));

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setLabelCol(labelName)
			.setPredictionDetailCol("detail")
			.linkFrom(predict)
			.collectMetrics();

		Assert.assertTrue(metrics.getAccuracy() > 0.9);

	}
}
