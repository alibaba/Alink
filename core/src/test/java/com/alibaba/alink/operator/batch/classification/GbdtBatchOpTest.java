package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Test cases for gbdt.
 */
public class GbdtBatchOpTest {

	@Test
	public void linkFrom() throws Exception {
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

		List<Row> result = model.getSideOutput(0).collect();

		double max = Double.NEGATIVE_INFINITY;

		for (Row row : result) {
			max = Math.max(max, (double) row.getField(1));
		}

		Assert.assertEquals(max, 2, 1e-6);
	}

	@Test
	public void linkFrom1() throws Exception {
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

	@Test
	public void testLazy() throws Exception {
		final TemporaryFolder temporaryFolder = new TemporaryFolder();

		try {
			temporaryFolder.create();
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
				.setLearningRate(0.1)
				.setNumTrees(2);

			gbdtTrainBatchOp.linkFrom(memSourceBatchOp).lazyPrintModelInfo();

			gbdtTrainBatchOp.linkFrom(memSourceBatchOp).lazyCollectModelInfo(modelInfo -> {
				System.out.println(modelInfo.getCaseWhenRule(0));
				try {
					modelInfo.saveTreeAsImage(temporaryFolder.getRoot().toPath().toString() + "/tree_model_image.png", 0, true);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});

			BatchOperator.execute();
		} finally {
			temporaryFolder.delete();
		}
	}
}