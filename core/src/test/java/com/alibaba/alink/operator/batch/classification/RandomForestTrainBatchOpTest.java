package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.regression.CartRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp;
import com.alibaba.alink.operator.batch.regression.RandomForestRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.C45PredictStreamOp;
import com.alibaba.alink.operator.stream.classification.CartPredictStreamOp;
import com.alibaba.alink.operator.stream.classification.Id3PredictStreamOp;
import com.alibaba.alink.operator.stream.classification.RandomForestPredictStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalMultiClassStreamOp;
import com.alibaba.alink.operator.stream.regression.CartRegPredictStreamOp;
import com.alibaba.alink.operator.stream.regression.RandomForestRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.C45;
import com.alibaba.alink.pipeline.classification.Cart;
import com.alibaba.alink.pipeline.classification.Id3;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Test cases for RandomForest
 */

public class RandomForestTrainBatchOpTest extends AlinkTestBase {
	MemSourceBatchOp input = null;
	MemSourceStreamOp inputStream = null;
	String[] colNames = null;
	String labelColName = null;
	String[] featureColNames = null;
	String[] categoricalColNames = null;

	@Before
	public void setUp() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1.0, "A", 0L, 0, 0, 1.0),
			Row.of(2.0, "B", 1L, 1, 0, 2.0),
			Row.of(3.0, "C", 2L, 2, 1, 3.0),
			Row.of(4.0, "D", 3L, 3, 1, 4.0)
		};

		colNames = new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"};

		featureColNames = new String[] {colNames[0], colNames[1], colNames[2], colNames[3]};

		categoricalColNames = new String[] {colNames[1]};

		labelColName = colNames[4];

		input = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"});
		inputStream = new MemSourceStreamOp(
			Arrays.asList(rows),
			new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"}
		);
	}

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
			.setCategoricalCols(colNames[1])
			.setMinSamplesPerLeaf(1)
			.setMaxDepth(4)
			.setMaxMemoryInMB(1)
			.setCreateTreeMode("parallel");

		DecisionTreeRegPredictBatchOp decisionTreeRegPredictBatchOp = new DecisionTreeRegPredictBatchOp()
			.setPredictionCol("pred");

		decisionTreeRegPredictBatchOp
			.linkFrom(
				decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp),
				memSourceBatchOp
			).lazyCollect();

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
			.setCategoricalCols(colNames[1])
			.setMinSamplesPerLeaf(1)
			.setMaxMemoryInMB(1)
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
			).collect();
	}

	@Test
	public void linkFromRandomForestClassifierParallel() throws Exception {
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

		RandomForestTrainBatchOp randomForestTrainBatchOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setCategoricalCols(colNames[1])
			.setMinSamplesPerLeaf(1)
			.setMaxMemoryInMB(1)
			.setFeatureSubsamplingRatio(0.1)
			.setNumTrees(2)
			.setCreateTreeMode("parallel");

		RandomForestPredictBatchOp randomForestPredictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred");

		EvalMultiClassBatchOp eval = new EvalMultiClassBatchOp()
			.setLabelCol(colNames[2])
			.setPredictionCol("pred");

		eval
			.linkFrom(
				randomForestPredictBatchOp
					.linkFrom(
						randomForestTrainBatchOp
							.linkFrom(
								memSourceBatchOp
							),
						memSourceBatchOp
					)
			).collect();
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

		decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp).collect();
	}

	@Test
	public void linkFrom7() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 1),
				Row.of(1, 2, 1),
				Row.of(0, 3, 0),
				Row.of(0, null, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 0),
				Row.of(4, 4, 0)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setFeatureSubsamplingRatio(1.0)
			.setSubsamplingRatio(1.0)
			.setNumTreesOfInfoGain(1)
			.setNumTreesOfInfoGain(1)
			.setNumTreesOfInfoGainRatio(1)
			.setCategoricalCols(colNames[1]);

		rfOp.linkFrom(memSourceBatchOp).collect();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred_result");

		predictBatchOp.linkFrom(rfOp.linkFrom(memSourceBatchOp), memSourceBatchOp).collect();
	}

	@Test
	public void testImportance() throws Exception {
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
			.setNumTreesOfInfoGain(1)
			.setNumTreesOfGini(1)
			.setNumTreesOfInfoGainRatio(1)
			.setCategoricalCols(colNames[0], colNames[1]);

		rfOp.linkFrom(memSourceBatchOp).getSideOutput(0).collect();
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
			.setNumTreesOfGini(1)
			.setCategoricalCols(colNames[0], colNames[1]);

		rfOp.linkFrom(memSourceBatchOp).collect();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred_result");

		predictBatchOp.linkFrom(rfOp.linkFrom(memSourceBatchOp), memSourceBatchOp).collect();
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

		rfOp.linkFrom(memSourceBatchOp).collect();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("pred_result");

		predictBatchOp.linkFrom(rfOp.linkFrom(memSourceBatchOp), memSourceBatchOp).collect();
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

		cartRegBatchOp.linkFrom(memSourceBatchOp).collect();

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("result")
			.setPredictionDetailCol("detail");

		predictBatchOp.linkFrom(cartRegBatchOp.linkFrom(memSourceBatchOp), memSourceBatchOp).collect();
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

		cartRegBatchOp.linkFrom(memSourceBatchOp);

		RandomForestPredictBatchOp predictBatchOp = new RandomForestPredictBatchOp()
			.setPredictionCol("result")
			.setPredictionDetailCol("detail");

		predictBatchOp.linkFrom(cartRegBatchOp.linkFrom(memSourceBatchOp), memSourceBatchOp).collect();
	}

	@Test
	public void testMissForContinuous() throws Exception {
		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			new ArrayList <>(Arrays.asList(
				Row.of(0.1, 0),
				Row.of(0.2, 1),
				Row.of(null, 0)
			)
			),
			new String[] {"f0", "label"}
		);

		RandomForestTrainBatchOp randomForestBatchOp = new RandomForestTrainBatchOp()
			.setLabelCol("label")
			.setFeatureCols("f0");

		randomForestBatchOp.linkFrom(memSourceBatchOp).collect();
	}

	@Test
	public void linkFrom1() {
		Random random = new Random();

		int fieldSize = 50;
		int arraySize = 10;

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

		decisionTreeRegTrainBatchOp.linkFrom(memSourceBatchOp).collect();
	}

	@Test
	public void testC45() throws Exception {
		C45TrainBatchOp c45BatchOp = new C45TrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName);

		C45PredictBatchOp c45PredictBatchOp = new C45PredictBatchOp()
			.setReservedCols(new String[] {labelColName})
			.setPredictionCol("c45_predict_result")
			.setPredictionDetailCol("c45_predict_detail");

		EvalMultiClassBatchOp evalClassificationBatchOp = new EvalMultiClassBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol("c45_predict_result")
			.setPredictionDetailCol("c45_predict_detail");

		C45PredictStreamOp c45PredictStreamOp = new C45PredictStreamOp(c45BatchOp)
			.setPredictionCol("c45_predict_result")
			.setPredictionDetailCol("c45_predict_detail");

		BatchOperator <?> c45Model = input.linkTo(c45BatchOp);
		BatchOperator <?> c45Pred = c45PredictBatchOp.linkFrom(c45Model, input);

		c45Pred.lazyCollect();

		BatchOperator <?> evalResult = evalClassificationBatchOp.linkFrom(c45Pred);

		evalResult.lazyCollect();

		c45PredictStreamOp.linkFrom(inputStream);

		EvalMultiClassStreamOp evalClassStreamOp = new EvalMultiClassStreamOp()
			.setLabelCol(labelColName)
			.setPredictionCol("c45_predict_result")
			.setPredictionDetailCol("c45_predict_detail");

		evalClassStreamOp.linkFrom(c45PredictStreamOp.linkFrom(inputStream));

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testId3() throws Exception {
		Id3TrainBatchOp id3BatchOp = new Id3TrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName);

		Id3PredictBatchOp id3PredictBatchOp = new Id3PredictBatchOp()
			.setReservedCols(new String[] {labelColName})
			.setPredictionCol("id3_predict_result")
			.setPredictionDetailCol("id3_predict_detail");

		EvalMultiClassBatchOp evalClassificationBatchOp = new EvalMultiClassBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol("id3_predict_result")
			.setPredictionDetailCol("id3_predict_detail");

		Id3PredictStreamOp id3PredictStreamOp = new Id3PredictStreamOp(id3BatchOp)
			.setPredictionCol("id3_predict_result")
			.setPredictionDetailCol("id3_predict_detail");

		BatchOperator <?> id3Model = input.linkTo(id3BatchOp);
		BatchOperator <?> id3Pred = id3PredictBatchOp.linkFrom(id3Model, input);

		BatchOperator <?> evalResult = evalClassificationBatchOp.linkFrom(id3Pred);

		evalResult.lazyCollect();

		id3PredictStreamOp.linkFrom(inputStream);

		EvalMultiClassStreamOp evalClassStreamOp = new EvalMultiClassStreamOp()
			.setLabelCol(labelColName)
			.setPredictionCol("id3_predict_result")
			.setPredictionDetailCol("id3_predict_detail");

		evalClassStreamOp.linkFrom(id3PredictStreamOp.linkFrom(inputStream));
		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testCart() throws Exception {
		CartTrainBatchOp cartBatchOp = new CartTrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName);

		CartPredictBatchOp cartPredictBatchOp = new CartPredictBatchOp()
			.setReservedCols(new String[] {labelColName})
			.setPredictionCol("cart_predict_result")
			.setPredictionDetailCol("cart_predict_detail");

		EvalMultiClassBatchOp evalClassificationBatchOp = new EvalMultiClassBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol("cart_predict_result")
			.setPredictionDetailCol("cart_predict_detail");

		CartPredictStreamOp cartPredictStreamOp = new CartPredictStreamOp(cartBatchOp)
			.setPredictionCol("cart_predict_result")
			.setPredictionDetailCol("cart_predict_detail");

		BatchOperator <?> cartModel = input.linkTo(cartBatchOp);
		BatchOperator <?> cartPred = cartPredictBatchOp.linkFrom(cartModel, input);

		cartPred.collect();

		BatchOperator <?> evalResult = evalClassificationBatchOp.linkFrom(cartPred);

		evalResult.collect();

		cartPredictStreamOp.linkFrom(inputStream);

		EvalMultiClassStreamOp evalClassStreamOp = new EvalMultiClassStreamOp()
			.setLabelCol(labelColName)
			.setPredictionCol("cart_predict_result")
			.setPredictionDetailCol("cart_predict_detail");

		evalClassStreamOp.linkFrom(cartPredictStreamOp.linkFrom(inputStream));

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testCartReg() throws Exception {
		CartRegTrainBatchOp cartRegBatchOp = new CartRegTrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(colNames[5]);

		CartRegPredictBatchOp cartRegPredictBatchOp = new CartRegPredictBatchOp()
			.setReservedCols(new String[] {colNames[5]})
			.setPredictionCol("cart_reg_predict_result");

		EvalRegressionBatchOp evalClassificationBatchOp = new EvalRegressionBatchOp()
			.setLabelCol(colNames[5])
			.setPredictionCol("cart_reg_predict_result");

		CartRegPredictStreamOp cartRegPredictStreamOp = new CartRegPredictStreamOp(cartRegBatchOp)
			.setPredictionCol("cart_reg_predict_result");

		BatchOperator <?> cartRegModel = input.linkTo(cartRegBatchOp);
		BatchOperator <?> cartRegPred = cartRegPredictBatchOp.linkFrom(cartRegModel, input);

		cartRegPred.collect();

		BatchOperator <?> evalResult = evalClassificationBatchOp.linkFrom(cartRegPred);

		evalResult.collect();

		cartRegPredictStreamOp.linkFrom(inputStream);

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testRandomForestsReg() throws Exception {
		RandomForestRegTrainBatchOp randomForestsRegBatchOp = new RandomForestRegTrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(colNames[5]);

		RandomForestRegPredictBatchOp randomForestsRegPredictBatchOp = new RandomForestRegPredictBatchOp()
			.setReservedCols(new String[] {colNames[5]})
			.setPredictionCol("random_forests_reg_predict_result");

		EvalRegressionBatchOp evalClassificationBatchOp = new EvalRegressionBatchOp()
			.setLabelCol(colNames[5])
			.setPredictionCol("random_forests_reg_predict_result");

		RandomForestRegPredictStreamOp randomForestsRegPredictStreamOp
			= new RandomForestRegPredictStreamOp(randomForestsRegBatchOp)
			.setPredictionCol("random_forests_reg_predict_result");

		BatchOperator <?> randomForestsRegModel = input.linkTo(randomForestsRegBatchOp);
		BatchOperator <?> randomForestsRegPred = randomForestsRegPredictBatchOp.linkFrom(randomForestsRegModel, input);

		randomForestsRegPred.collect();

		BatchOperator <?> evalResult = evalClassificationBatchOp.linkFrom(randomForestsRegPred);

		evalResult.collect();

		randomForestsRegPredictStreamOp.linkFrom(inputStream);

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void linkFromEval() throws Exception {
		RandomForestTrainBatchOp randomForestBatchOp = new RandomForestTrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName);

		RandomForestPredictBatchOp randomForestPredictBatchOp
			= new RandomForestPredictBatchOp()
			.setReservedCols(new String[] {labelColName})
			.setPredictionCol("rf_predict_result")
			.setPredictionDetailCol("rf_predict_result_detail");

		EvalMultiClassBatchOp evalClassificationBatchOp = new EvalMultiClassBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol("rf_predict_result")
			.setPredictionDetailCol("rf_predict_result_detail");

		RandomForestPredictStreamOp randomForestPredictStreamOp = new RandomForestPredictStreamOp(
			randomForestBatchOp)
			.setReservedCols(new String[] {labelColName})
			.setPredictionCol("rf_predict_result")
			.setPredictionDetailCol("rf_predict_result_detail");

		BatchOperator <?> rfModel = input.linkTo(randomForestBatchOp);
		BatchOperator <?> rfPred = randomForestPredictBatchOp.linkFrom(rfModel, input);

		BatchOperator <?> evalResult = evalClassificationBatchOp.linkFrom(rfPred);

		evalResult.lazyCollect();

		randomForestPredictStreamOp.linkFrom(inputStream);

		EvalMultiClassStreamOp evalClassStreamOp = new EvalMultiClassStreamOp()
			.setLabelCol(labelColName)
			.setPredictionCol("rf_predict_result")
			.setPredictionDetailCol("rf_predict_result_detail");

		evalClassStreamOp.linkFrom(randomForestPredictStreamOp.linkFrom(inputStream));

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testC45Pipeline() throws Exception {
		C45 c45 = new C45()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("c45_test_result")
			.setPredictionDetailCol("c45_test_detail");

		Pipeline pipeline = new Pipeline().add(c45);

		BatchOperator <?> output = pipeline.fit(input).transform(input);

		output.lazyPrint(-1);

		BatchOperator <?> output1 = BatchOperator.fromTable(output.getOutputTable());

		output1.lazyPrint(-1);

		AlgoOperator <?> outputStream = pipeline.fit(input).transform(inputStream);

		outputStream.print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testId3Pipeline() throws Exception {
		Id3 id3 = new Id3()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("id3_test_result")
			.setPredictionDetailCol("id3_test_detail");
		Pipeline pipeline = new Pipeline().add(id3);

		BatchOperator <?> output = pipeline.fit(input).transform(input);

		output.lazyPrint(-1);

		BatchOperator <?> output1 = BatchOperator.fromTable(output.getOutputTable());

		output1.lazyPrint(-1);

		StreamOperator <?> outputStream = pipeline.fit(input).transform(inputStream);

		outputStream.print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testCartPipeline() throws Exception {
		Cart cart = new Cart()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("cart_test_result")
			.setPredictionDetailCol("cart_test_detail");

		Pipeline pipeline = new Pipeline().add(cart);

		BatchOperator <?> output = pipeline.fit(input).transform(input);

		output.lazyPrint(-1);

		BatchOperator <?> output1 = BatchOperator.fromTable(output.getOutputTable());

		output1.lazyPrint(-1);

		AlgoOperator <?> outputStream = pipeline.fit(input).transform(inputStream);

		outputStream.print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testCartPipelineWithModel() throws Exception {
		String pipelineFile = folder.newFile().getAbsolutePath();
		String modelFile = folder.newFile().getAbsolutePath();

		Cart cart = new Cart()
			.setModelFilePath(modelFile)
			.setOverwriteSink(true)
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("cart_test_result")
			.setPredictionDetailCol("cart_test_detail");

		Pipeline pipeline = new Pipeline().add(cart);

		PipelineModel pipelineModel = pipeline.fit(input);

		pipelineModel.save(pipelineFile, true);

		BatchOperator.execute();

		pipelineModel = PipelineModel.load(pipelineFile);

		StreamOperator <?> outputStream = pipelineModel.transform(inputStream);

		outputStream.print();

		StreamOperator.execute();
	}

	@Test
	public void testCartWithModel() throws Exception {
		String modelFile = folder.newFile().getAbsolutePath();

		CartTrainBatchOp cartTrainBatchOp = new CartTrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName);

		input
			.link(cartTrainBatchOp)
			.link(
				new AkSinkBatchOp()
					.setFilePath(modelFile)
					.setOverwriteSink(true)
			);

		BatchOperator.execute();

		CartPredictStreamOp cartPredictStreamOp = new CartPredictStreamOp()
			.setPredictionCol("cart_test_result")
			.setPredictionDetailCol("cart_test_detail")
			.setModelFilePath(modelFile);

		inputStream.link(cartPredictStreamOp).print();

		StreamOperator.execute();
	}


	@Test
	public void testCartWithAkSourceModel() throws Exception {
		String modelFile = folder.newFile().getAbsolutePath();

		CartTrainBatchOp cartTrainBatchOp = new CartTrainBatchOp()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName);

		input
			.link(cartTrainBatchOp)
			.link(
				new AkSinkBatchOp()
					.setFilePath(modelFile)
					.setOverwriteSink(true)
			);

		BatchOperator.execute();

		CartPredictStreamOp cartPredictStreamOp = new CartPredictStreamOp(new AkSourceBatchOp().setFilePath(modelFile))
			.setPredictionCol("cart_test_result")
			.setPredictionDetailCol("cart_test_detail")
			.setModelFilePath(modelFile);

		inputStream.link(cartPredictStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void testCartWithPipelineModel() throws Exception {
		String pipelineFile = folder.newFile().getAbsolutePath();
		String modelFile = folder.newFile().getAbsolutePath();

		Cart cart = new Cart()
			.setModelFilePath(modelFile)
			.setOverwriteSink(true)
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("cart_test_result")
			.setPredictionDetailCol("cart_test_detail");

		Pipeline pipeline = new Pipeline().add(cart);

		PipelineModel pipelineModel = pipeline.fit(input);

		pipelineModel.save(pipelineFile, true);

		BatchOperator.execute();

		pipelineModel = PipelineModel.load(pipelineFile);

		pipelineModel.transform(input).print();
	}
}