package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp;
import com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 * Test cases for RandomForest
 */
public class RandomForestTrainBatchOpTest {

	@Test
	public void linkFrom() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		DecisionTreeRegTrainBatchOp decisionTreeRegTrainBatchOp = new DecisionTreeRegTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMinSamplesPerLeaf(1);

		DecisionTreeRegPredictBatchOp decisionTreeRegPredictBatchOp = new DecisionTreeRegPredictBatchOp()
			.setPredictionCol("pred");

		EvalRegressionBatchOp eval = new EvalRegressionBatchOp()
			.setLabelCol(colNames[2])
			.setPredictionCol("pred");

		Assert.assertEquals(
			new RegressionMetrics(
				decisionTreeRegPredictBatchOp
					.linkFrom(
						decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp),
						memSourceBatchOp
					)
					.linkTo(eval)
					.collect()
					.get(0)
			).getRmse(),
			0.026726,
			1e-6);
	}


	@Test
	public void linkFromDecisionTreeModeParallel() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		DecisionTreeRegTrainBatchOp decisionTreeRegTrainBatchOp = new DecisionTreeRegTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMinSamplesPerLeaf(1)
			.setCreateTreeMode("parallel");

		DecisionTreeRegPredictBatchOp decisionTreeRegPredictBatchOp = new DecisionTreeRegPredictBatchOp()
			.setPredictionCol("pred");

		EvalRegressionBatchOp eval = new EvalRegressionBatchOp()
			.setLabelCol(colNames[2])
			.setPredictionCol("pred");

		Assert.assertEquals(
			new RegressionMetrics(
				decisionTreeRegPredictBatchOp
					.linkFrom(
						decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp),
						memSourceBatchOp
					)
					.linkTo(eval)
					.collect()
					.get(0)
			).getRmse(),
			0.026726,
			1e-6);
	}


	@Test
	public void linkFromDecisionTreeClassifierParallel() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		DecisionTreeTrainBatchOp decisionTreeTrainBatchOp = new DecisionTreeTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMinSamplesPerLeaf(1)
			.setCreateTreeMode("parallel");

		DecisionTreeRegPredictBatchOp decisionTreeRegPredictBatchOp = new DecisionTreeRegPredictBatchOp()
			.setPredictionCol("pred");

		EvalMultiClassBatchOp eval = new EvalMultiClassBatchOp()
			.setLabelCol(colNames[2])
			.setPredictionCol("pred");

		eval
			.linkFrom(
				decisionTreeRegPredictBatchOp
					.linkFrom(
						decisionTreeTrainBatchOp
							.linkFrom(
								memSourceBatchOp
							),
						memSourceBatchOp
					)
			)
			.print();
	}

	@Test
	public void linkFrom6() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		DecisionTreeRegTrainBatchOp decisionTreeRegTrainBatchOp = new DecisionTreeRegTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1]);

		decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp).print();
	}

	@Test
	public void linkFrom7() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setNumTrees(3)
			.setTreeType("1,2")
			.setCategoricalCols(colNames[0], colNames[1]);

		rfOp.linkFrom(memSourceBatchOp).print();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred_result");

		predictBatchOp.linkFrom(rfOp.linkFrom(memSourceBatchOp), memSourceBatchOp).print();
	}

	@Test
	public void linkFrom2() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setNumTrees(1)
			.setTreeType("gini")
			.setCategoricalCols(colNames[0], colNames[1]);

		rfOp.linkFrom(memSourceBatchOp).print();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred_result");

		predictBatchOp.linkFrom(rfOp.linkFrom(memSourceBatchOp), memSourceBatchOp).print();
	}

	@Test
	public void linkFrom3() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMaxDepth(10)
			.setNumTrees(10);

		rfOp.linkFrom(memSourceBatchOp).print();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred_result");

		predictBatchOp.linkFrom(rfOp.linkFrom(memSourceBatchOp), memSourceBatchOp).print();
	}

	@Test
	public void linkFrom4() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp cartRegBatchOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMaxDepth(10)
			.setNumTrees(10)
			.setSubsamplingRatio(0.6)
			.setNumSubsetFeatures(1);

		cartRegBatchOp.linkFrom(memSourceBatchOp).print();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("result")
			.setPredictionDetailCol("detail");

		predictBatchOp.linkFrom(cartRegBatchOp.linkFrom(memSourceBatchOp), memSourceBatchOp).print();
	}

	@Test
	public void linkFrom5() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(0, 2, 1),
				Row.of(0, 3, 0),
				Row.of(1, 2, 0),
				Row.of(1, 4, 1),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 0)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp cartRegBatchOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMaxDepth(10)
			.setNumTrees(1000)
			.setSubsamplingRatio(0.6)
			.setNumSubsetFeatures(1);

		cartRegBatchOp.linkFrom(memSourceBatchOp).print();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("result")
			.setPredictionDetailCol("detail");

		predictBatchOp.linkFrom(cartRegBatchOp.linkFrom(memSourceBatchOp), memSourceBatchOp).print();
	}

	@Test
	public void linkFrom1() throws Exception {
		Random random = new Random();

		int fieldSize = 50;
		int arraySize = 10000;

		Row[] testArray = new Row[arraySize];

		for (int i = 0; i < arraySize; ++i) {
			testArray[i] = new Row(fieldSize);

			for (int j = 0; j < fieldSize; ++j) {
				testArray[i].setField(j, random.nextDouble());
			}
		}

		String[] colNames = new String[fieldSize];

		for (int i = 0; i < fieldSize; ++i) {
			colNames[i] = "f" + i;
		}

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestRegTrainBatchOp decisionTreeRegTrainBatchOp = new RandomForestRegTrainBatchOp()
			.setLabelCol(colNames[0])
			.setFeatureCols(Arrays.copyOfRange(colNames, 1, colNames.length))
			.setMaxDepth(10)
			.setNumSubsetFeatures(2)
			.setNumTrees(3);

		decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp).print();
	}
}