package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.feature.BinningTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.optim.ConstraintBetweenBins;
import com.alibaba.alink.operator.common.optim.FeatureConstraint;
import com.alibaba.alink.params.feature.HasEncode;
import com.alibaba.alink.params.finance.BinningTrainParams;
import com.alibaba.alink.params.finance.HasConstrainedLinearModelType;
import com.alibaba.alink.params.finance.HasConstrainedLinearModelType.LinearModelType;
import com.alibaba.alink.params.finance.HasConstrainedOptimizationMethod.ConstOptimMethod;
import com.alibaba.alink.params.finance.HasOdds;
import com.alibaba.alink.params.finance.HasPdo;
import com.alibaba.alink.params.finance.HasScaledValue;
import com.alibaba.alink.params.finance.ScorecardTrainParams;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;

public class ScorecardTrainBatchOpTest extends AlinkTestBase {

	private static final Row[] vecrows = new Row[] {
		Row.of("$3$0:1.0 1:7.0 2:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 1),
		Row.of("$3$0:1.0 1:3.0 2:3.0", "2.0 3.0 3.0", 2.0, 3.0, 3.0, 1),
		Row.of("$3$0:1.0 1:2.0 2:4.0", "3.0 2.0 4.0", 3.0, 2.0, 4.0, 1),
		Row.of("$3$0:1.0 1:3.0 2:4.0", "2.0 3.0 4.0", 2.0, 3.0, 4.0, 1),
		Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 5.0 8.0", 1.0, 5.0, 8.0, 0),
		Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 6.0 3.0", 1.0, 6.0, 3.0, 0),
		Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 5.0 8.0", 1.0, 5.0, 8.0, 0),
		Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 6.0 3.0", 1.0, 6.0, 3.0, 0)
	};

	private static final String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};
	private static final Row[] array = new Row[] {
		Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
		Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
		Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
		Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
		Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
		Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
		Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
		Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0)

	};
	private static final String[] lrColNames = new String[] {"f0", "f1", "f2", "f3"};
	private BatchOperator <?> lrData;

	@Before
	public void init() {
		lrData = new MemSourceBatchOp(
			Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "label"});
	}

	@Test
	public void testOneModel() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		BatchOperator <?> scorecard =
			new ScorecardTrainBatchOp()
				.setConstOptimMethod("alm")
				.setSelectedCols(lrColNames)
				.setLinearModelType("divergence")
				.setPositiveLabelValueString("1")
				.setLabelCol("label")
				.linkFrom(lrData);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setReservedCols("label")
			.setPredictionDetailCol("detail")
			.setPredictionScoreCol("score")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	private void testOneModelLinearReg() throws Exception {
		BatchOperator scorecard =
			new ScorecardTrainBatchOp()
				.setConstOptimMethod("sqp")
				.setSelectedCols(lrColNames)
				.setLinearModelType(HasConstrainedLinearModelType.LinearModelType.LR)
				.setPositiveLabelValueString("1")
				.setLabelCol("label")
				.linkFrom(lrData);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setReservedCols("label")
			.setPredictionDetailCol("detail")
			.setPredictionScoreCol("score")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	private void testOneModelWithStepwise() throws Exception {
		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setConstOptimMethod("sqp")
				.setSelectedCols(lrColNames)
				.setLinearModelType(HasConstrainedLinearModelType.LinearModelType.LR)
				.setLabelCol("label")
				.setWithSelector(true)
				.setPositiveLabelValueString("0")
				.setEncode(ScorecardTrainParams.Encode.WOE)
				.linkFrom(lrData, null);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setReservedCols("label")
			.setPredictionDetailCol("detail")
			.setPredictionScoreCol("score")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	//todo:
	private void testOneModelWithStepwiseLinearReg() throws Exception {
		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setConstOptimMethod("sqp")
				.setSelectedCols(lrColNames)
				.setLinearModelType(HasConstrainedLinearModelType.LinearModelType.LinearReg)
				.setLabelCol("label")
				.setWithSelector(true)
				.setForceSelectedCols(new String[] {"f2", "f1"})
				.setPositiveLabelValueString("0")
				.setEncode(ScorecardTrainParams.Encode.WOE)
				.linkFrom(lrData, null);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setReservedCols("label")
			.setPredictionDetailCol("detail")
			.setPredictionScoreCol("score")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				System.out.println(metrics.getAuc());
				Assert.assertTrue(metrics.getAuc() > 0.8);
			}
		});
	}

	private void testTwoLinearModels() throws Exception {
		BatchOperator scorecard = new ScorecardTrainBatchOp()
			.setSelectedCols(lrColNames)
			.setLabelCol("label")
			.setScaleInfo(true)
			.setScaledValue(80.0)
			.setOdds(20.0)
			.setPdo(10.0)
			.linkFrom(lrData);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setReservedCols("label")
			.setPredictionScoreCol("score")
			.setPredictionDetailCol("detail")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	//@Test
	public void testThreeModels() throws Exception {
		ConstraintBetweenBins bfc = new ConstraintBetweenBins();
		bfc.name = "f0";
		bfc.addEqual(new Number[] {1, 1.12345});
		bfc.addLargerThan(new Number[] {2, 1.8});
		bfc.addLessThanBin(new Number[] {2, 3});
		FeatureConstraint f = new FeatureConstraint();

		f.addBinConstraint(bfc);

		//ConstraintBetweenBins bfc2 = new ConstraintBetweenBins();
		//bfc2.name = "f1";
		//bfc2.addLargerThanBin(new Number[] {1, 2});
		//bfc2.addScale(new Number[] {0, 1, 2.0});
		//bfc2.addLessThan(new Number[] {3, 3.2});
		//
		//f.addBinConstraint(bfc2);

		BatchOperator <?> constraints =
			new MemSourceBatchOp(Collections.singletonList(Row.of(f.toString())),
				new String[] {"constraint"});

		BinningTrainBatchOp binning = new BinningTrainBatchOp()
			.setBinningMethod(BinningTrainParams.BinningMethod.BUCKET)
			.setNumBuckets(4)
			.setSelectedCols(lrColNames)
			.linkFrom(lrData);

		lrData.lazyPrint("----data----");
		binning.lazyPrint("----binning----");
		constraints.lazyPrint("----constraints----");

		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setScaleInfo(true)
				.setScaledValue(80.0)
				.setOdds(20.0)
				.setPdo(10.0)
				.setSelectedCols("f0", "f1", "f2")
				.setLabelCol("label")
				.setConstOptimMethod(ConstOptimMethod.SQP)
				.linkFrom(lrData, binning, constraints);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setPredictionScoreCol("score")
			.setPredictionDetailCol("detail")
			.setReservedCols("label")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});

		//BatchOperator.execute();
	}

	private void testThreeModelsLinearReg() throws Exception {
		ConstraintBetweenBins bfc = new ConstraintBetweenBins();
		bfc.name = "f0";
		bfc.addEqual(new Number[] {1, 1.12345});
		//        bfc.addLargerThan(new Number[]{2, 1.8});
		FeatureConstraint f = new FeatureConstraint();
		f.addBinConstraint(bfc);
		BatchOperator <?> constraints = new MemSourceBatchOp(Collections.singletonList(Row.of(f.toString())),
			new String[] {"constraint"});

		BinningTrainBatchOp binning = new BinningTrainBatchOp()
			.setBinningMethod(BinningTrainParams.BinningMethod.BUCKET)
			.setNumBuckets(2)
			.setSelectedCols(lrColNames)
			.linkFrom(lrData);

		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setScaleInfo(true)
				.setScaledValue(80.0)
				.setOdds(20.0)
				.setPdo(10.0)
				.setSelectedCols("f0", "f1", "f2")
				.setLinearModelType(LinearModelType.LR)
				.setLabelCol("label")
				.linkFrom(lrData, binning, constraints);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setPredictionScoreCol("score")
			.setPredictionDetailCol("detail")
			.setReservedCols("label")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	private void testThreeModelsStepwise() throws Exception {
		ConstraintBetweenBins bfc = new ConstraintBetweenBins();
		bfc.name = "f0";
		bfc.addEqual(new Number[] {1, 1.12345});
		//        bfc.addLargerThan(new Number[]{2, 1.8});
		FeatureConstraint f = new FeatureConstraint();
		f.addBinConstraint(bfc);
		BatchOperator constraints = new MemSourceBatchOp(Collections.singletonList(Row.of(f.toString())),
			new String[] {"constraint"});

		BinningTrainBatchOp binning = new BinningTrainBatchOp()
			.setBinningMethod(BinningTrainParams.BinningMethod.BUCKET)
			.setNumBuckets(2)
			.setSelectedCols(lrColNames)
			.setLabelCol("label")
			.setPositiveLabelValueString("1")
			.linkFrom(lrData);

		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setScaleInfo(true)
				.setScaledValue(80.0)
				.setOdds(20.0)
				.setPdo(10.0)
				.setSelectedCols("f0", "f1", "f2")
				.setLabelCol("label")
				.setWithSelector(true)
				.setEncode(ScorecardTrainParams.Encode.WOE)
				.setDefaultWoe(0.1)
				.linkFrom(lrData, binning, constraints);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setPredictionScoreCol("score")
			.setPredictionDetailCol("detail")
			.setReservedCols("label")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	private void testThreeModelsStepwiseVector() throws Exception {
		BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);

		ConstraintBetweenBins bfc = new ConstraintBetweenBins();
		bfc.name = "f0";
		bfc.addEqual(new Number[] {1, 1.12345});
		//        bfc.addLargerThan(new Number[]{2, 1.8});
		FeatureConstraint f = new FeatureConstraint();
		f.addBinConstraint(bfc);
		BatchOperator constraints = new MemSourceBatchOp(Collections.singletonList(Row.of(f.toString())),
			new String[] {"constraint"});

		BinningTrainBatchOp binning = new BinningTrainBatchOp()
			.setBinningMethod("QUANTILE")
			.setDiscreteThresholds(3)
			.setSelectedCols("f0", "f1", "f2")
			.setLabelCol("label")
			.setPositiveLabelValueString("1")
			.linkFrom(vecdata);

		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setScaleInfo(true)
				.setScaledValue(80.0)
				.setOdds(20.0)
				.setPdo(10.0)
				.setSelectedCols("f0", "f1", "f2")
				.setLabelCol("label")
				.setWithSelector(true)
				.setEncode(ScorecardTrainParams.Encode.ASSEMBLED_VECTOR)
				.setDefaultWoe(0.1)
				.linkFrom(vecdata, binning, constraints);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setPredictionScoreCol("score")
			.setPredictionDetailCol("detail")
			.setReservedCols("label");

		predict.linkFrom(scorecard, vecdata);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.8);
				Assert.assertTrue(metrics.getAuc() > 0.8);
			}
		});
	}

	private void testWoe() throws Exception {
		BinningTrainBatchOp binning = new BinningTrainBatchOp()
			.setBinningMethod(BinningTrainParams.BinningMethod.BUCKET)
			.setLabelCol("label")
			.setPositiveLabelValueString("1")
			.setNumBuckets(2)
			.setSelectedCols(lrColNames)
			.linkFrom(lrData);

		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setEncode(HasEncode.Encode.WOE.name())
				.setDefaultWoe(1.5)
				.setScaleInfo(true)
				.setScaledValue(800.0)
				.setOdds(50.0)
				.setPdo(40.0)
				.setSelectedCols(lrColNames)
				.setLabelCol("label")
				.linkFrom(lrData, binning);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setPredictionDetailCol("detail")
			.setPredictionScoreCol("score")
			.setReservedCols("label")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	private void testEncodeNULL() throws Exception {
		BinningTrainBatchOp binning = new BinningTrainBatchOp()
			.setBinningMethod(BinningTrainParams.BinningMethod.BUCKET)
			.setNumBuckets(2)
			.setSelectedCols("f0", "f1", "f2")
			.linkFrom(lrData);

		ScorecardTrainBatchOp scorecard =
			new ScorecardTrainBatchOp()
				.setEncode(ScorecardTrainParams.Encode.NULL.name())
				.setScaleInfo(true)
				.setScaledValue(80.0)
				.setOdds(20.0)
				.setPdo(10.0)
				.setSelectedCols(lrColNames)
				.setLabelCol("label")
				.linkFrom(lrData, binning);

		ScorecardPredictBatchOp predict = new ScorecardPredictBatchOp()
			.setCalculateScorePerFeature(true)
			.setPredictionScoreCol("score")
			.setPredictionDetailCol("detail")
			.setReservedCols("label")
			.linkFrom(scorecard, lrData);

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(predict);

		eval.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
			@Override
			public void accept(BinaryClassMetrics metrics) {
				Assert.assertTrue(metrics.getAccuracy() > 0.9);
				Assert.assertTrue(metrics.getAuc() > 0.9);
			}
		});
	}

	@Test
	public void testOneAndTwoModels() throws Exception {
		testOneModelLinearReg();
		testOneModelWithStepwise();
		testOneModelWithStepwiseLinearReg();
		testTwoLinearModels();
		testThreeModels();
		BatchOperator.execute();
	}

	@Test
	public void testMoreModels() throws Exception {
		testThreeModelsLinearReg();
		testThreeModelsStepwise();
		testThreeModelsStepwiseVector();
		BatchOperator.execute();
	}

	@Test
	public void testEncode() throws Exception {
		testWoe();
		testEncodeNULL();
		BatchOperator.execute();
	}

	@Test
	public void testTransformIsModelSet() {
		String[] selectedCols = new String[] {"f0", "f1", "f2"};
		Tuple2 <String[], TypeInformation <?>[]> t = PipelineModelMapper
			.getExtendModelSchema(
				PipelineModelMapper.getExtendModelSchema(
					new TableSchema(new String[] {"id", "p0"}, new TypeInformation <?>[] {Types.LONG, Types.STRING}),
					selectedCols,
					new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING}
				)
			);
		Assert.assertArrayEquals(selectedCols, t.f0);
	}

	@Test
	public void testScaleLinearModelWeight() {
		Row[] rows = new Row[] {
			Row.of(0L,
				"{\"hasInterceptItem\":\"true\",\"modelName\":\"\\\"Logistic Regression\\\"\",\"labelType\":\"4\","
					+ "\"modelSchema\":\"\\\"model_id bigint,model_info string,label_type int\\\"\","
					+ "\"isNewFormat\":\"true\",\"linearModelType\":\"\\\"LR\\\"\"}", null),
			Row.of(1048576L, "{\"featureColNames\":[\"f0\",\"f1\",\"f2\"],"
				+ "\"coefVector\":{\"data\":[-37.684,10.135,0.0,26.121]}}", null),
			Row.of(Integer.MAX_VALUE * 1048576L + 0L, null, 1),
			Row.of(Integer.MAX_VALUE * 1048576L + 1L, null, 0),
		};

		LinearModelData linearModelData = new LinearModelDataConverter().load(Arrays.asList(rows));
		Params params = new Params().set(HasScaledValue.SCALED_VALUE, 800.0).set(HasOdds.ODDS, 50.0).set(HasPdo.PDO,
			40.0);
		LinearModelData scaleModel = ScorecardTrainBatchOp.scaleLinearModelWeight(linearModelData,
			ScorecardTrainBatchOp
				.loadScaleInfo(params));
		Assert.assertEquals(scaleModel.coefVector.get(0), -1600.4, 0.1);
		Assert.assertEquals(scaleModel.coefVector.get(1), 584.9, 0.1);
		Assert.assertEquals(scaleModel.coefVector.get(2), 0.0, 0.1);
		Assert.assertEquals(scaleModel.coefVector.get(3), 1507.4, 0.1);
	}

	@Test
	public void testLoadScaleInfo() {
		Params params = new Params().set(HasScaledValue.SCALED_VALUE, 800.0).set(HasOdds.ODDS, 50.0).set(HasPdo.PDO,
			40.0);
		Tuple2 <Double, Double> t = ScorecardTrainBatchOp.loadScaleInfo(params);
		Assert.assertEquals(t.f0, 0.017329, 0.0001);
		Assert.assertEquals(t.f1, -9.950921, 0.0001);
	}

}
