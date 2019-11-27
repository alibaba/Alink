package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test cases for gbdt.
 */
public class GbdtBatchOpTest {

	@Test
	public void linkFrom() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(1);
		Row[] testArray =
			new Row[]{
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1)
			};

		String[] colNames = new String[]{"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMaxDepth(2)
			.setMinSamplesPerLeaf(1)
			.setNumTrees(2);

		BatchOperator<?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		Assert.assertEquals((long) (((Row) model.getSideOutput(0).collect().get(0)).getField(1)), 2);
	}

	@Test
	public void linkFrom1() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(1);
		Row[] testArray =
			new Row[]{
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1)
			};

		String[] colNames = new String[]{"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMinSamplesPerLeaf(1)
			.setNumTrees(2);

		BatchOperator<?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		Assert.assertEquals(
			new GbdtPredictBatchOp()
				.setPredictionCol("pred_col")
				.setPredictionDetailCol("pred_detail")
				.linkFrom(
					model,
					memSourceBatchOp
				)
				.link(
					new EvalBinaryClassBatchOp()
						.setLabelCol(colNames[2])
						.setPositiveLabelValueString("1")
						.setPredictionDetailCol("pred_detail")
				)
				.collectMetrics()
				.getAuc(),
			1.0,
			1e-6
		);
	}
}