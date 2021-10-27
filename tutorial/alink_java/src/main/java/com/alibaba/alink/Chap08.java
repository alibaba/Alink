package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LinearSvmPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.LinearSvmTrainBatchOp;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp;
import com.alibaba.alink.operator.batch.statistics.SummarizerBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.shared.linear.LinearTrainParams.OptimMethod;
import com.alibaba.alink.params.statistics.HasMethod.Method;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.FmClassifier;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorPolynomialExpand;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class Chap08 {

	static final String DATA_DIR = Utils.ROOT_DIR + "banknote" + File.separator;

	static final String ORIGIN_FILE = "data_banknote_authentication.txt";

	static final String SCHEMA_STRING
		= "variance double, skewness double, kurtosis double, entropy double, class int";

	static final String TRAIN_FILE = "train.ak";
	static final String TEST_FILE = "test.ak";
	static final String LR_PRED_FILE = "lr_pred.ak";
	static final String SVM_PRED_FILE = "svm_pred.ak";

	static final String[] FEATURE_COL_NAMES = new String[] {"variance", "skewness", "kurtosis", "entropy"};
	static final String LABEL_COL_NAME = "class";

	static final String VEC_COL_NAME = "vec";

	static final String PREDICTION_COL_NAME = "pred";
	static final String PRED_DETAIL_COL_NAME = "predinfo";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_3();

		c_3_1();

		c_3_2();

		c_4();

		c_5();

		c_6();

		c_7();

		c_8();

		c_9();

	}

	static void c_3() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		System.out.println("schema of source:");
		System.out.println(source.getSchema());

		System.out.println("column names of source:");
		System.out.println(ArrayUtils.toString(source.getColNames()));

		System.out.println("column types of source:");
		System.out.println(ArrayUtils.toString(source.getColTypes()));

		source.firstN(5).print();

	}

	static void c_3_1() throws Exception {

		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		TableSummary summary = new SummarizerBatchOp().linkFrom(source).collectSummary();
		System.out.println("Count of data set : " + summary.count());
		System.out.println("Max value of entropy : " + summary.max("entropy"));
		System.out.println(summary);

		source
			.link(
				new SummarizerBatchOp()
					.lazyCollectSummary(
						new Consumer <TableSummary>() {
							@Override
							public void accept(TableSummary tableSummary) {
								System.out.println("Count of data set : " + tableSummary.count());
								System.out.println("Max value of entropy : " + tableSummary.max("entropy"));
								System.out.println(tableSummary);
							}
						}
					)
			);

		source
			.link(
				new SummarizerBatchOp()
					.lazyPrintSummary()
			);

		source
			.lazyPrintStatistics("<- origin data ->")
			.firstN(5)
			.lazyPrintStatistics("<- first 5 data ->")
			.print();

	}

	static void c_3_2() throws Exception {
		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setFilePath(DATA_DIR + ORIGIN_FILE)
			.setSchemaStr(SCHEMA_STRING);

		CorrelationResult correlation = new CorrelationBatchOp().linkFrom(source).collectCorrelation();
		String[] colNames = correlation.getColNames();
		System.out.print("Correlation of " + colNames[0] + " with " + colNames[1]);
		System.out.println(" is " + correlation.getCorrelation()[0][1]);
		System.out.println(correlation.getCorrelationMatrix());

		source
			.link(
				new CorrelationBatchOp()
					.lazyCollectCorrelation(new Consumer <CorrelationResult>() {
						@Override
						public void accept(CorrelationResult correlationResult) {
							String[] colNames = correlationResult.getColNames();
							System.out.print("Correlation of " + colNames[0] + " with " + colNames[1]);
							System.out.println(" is " + correlationResult.getCorrelation()[0][1]);
							System.out.println(correlationResult.getCorrelationMatrix());
						}
					})
			);

		source
			.link(
				new CorrelationBatchOp()
					.lazyPrintCorrelation("< Pearson Correlation >")
			);

		source.link(
			new CorrelationBatchOp()
				.setMethod(Method.SPEARMAN)
				.lazyPrintCorrelation("< Spearman Correlation >")
		);

		BatchOperator.execute();

	}

	static void c_4() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		Utils.splitTrainTestIfNotExist(
			source,
			DATA_DIR + TRAIN_FILE,
			DATA_DIR + TEST_FILE,
			0.8
		);
	}

	static void c_5() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		LogisticRegressionTrainBatchOp lrTrainer =
			new LogisticRegressionTrainBatchOp()
				.setFeatureCols(FEATURE_COL_NAMES)
				.setLabelCol(LABEL_COL_NAME);

		LogisticRegressionPredictBatchOp lrPredictor =
			new LogisticRegressionPredictBatchOp()
				.setPredictionCol(PREDICTION_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(lrTrainer);

		lrPredictor.linkFrom(lrTrainer, test_data);

		lrTrainer.lazyPrintTrainInfo().lazyPrintModelInfo();

		lrPredictor
			.lazyPrint(5, "< Prediction >")
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + LR_PRED_FILE)
					.setOverwriteSink(true)
			);

		BatchOperator.execute();

	}

	static void c_6() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		LinearSvmTrainBatchOp svmTrainer =
			new LinearSvmTrainBatchOp()
				.setFeatureCols(FEATURE_COL_NAMES)
				.setLabelCol(LABEL_COL_NAME);

		LinearSvmPredictBatchOp svmPredictor =
			new LinearSvmPredictBatchOp()
				.setPredictionCol(PREDICTION_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(svmTrainer);

		svmPredictor.linkFrom(svmTrainer, test_data);

		svmTrainer.lazyPrintTrainInfo().lazyPrintModelInfo();

		svmPredictor
			.lazyPrint(5, "< Prediction >")
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + SVM_PRED_FILE)
					.setOverwriteSink(true)
			);

		BatchOperator.execute();

	}

	static void c_7() throws Exception {
		BinaryClassMetrics lr_metrics =
			new EvalBinaryClassBatchOp()
				.setPositiveLabelValueString("1")
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
				.linkFrom(
					new AkSourceBatchOp().setFilePath(DATA_DIR + LR_PRED_FILE)
				)
				.collectMetrics();

		StringBuilder sbd = new StringBuilder();
		sbd.append("< LR >\n")
			.append("AUC : ").append(lr_metrics.getAuc())
			.append("\t Accuracy : ").append(lr_metrics.getAccuracy())
			.append("\t Precision : ").append(lr_metrics.getPrecision())
			.append("\t Recall : ").append(lr_metrics.getRecall())
			.append("\n");
		System.out.println(sbd.toString());

		System.out.println(lr_metrics);

		lr_metrics.saveRocCurveAsImage(DATA_DIR + "lr_roc.jpg", true);
		lr_metrics.saveRecallPrecisionCurveAsImage(DATA_DIR + "lr_recallprec.jpg", true);
		lr_metrics.saveLiftChartAsImage(DATA_DIR + "lr_lift.jpg", true);
		lr_metrics.saveKSAsImage(DATA_DIR + "lr_ks.jpg", true);

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + SVM_PRED_FILE)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics()
					.lazyCollectMetrics(new Consumer <BinaryClassMetrics>() {
						@Override
						public void accept(BinaryClassMetrics binaryClassMetrics) {
							try {
								binaryClassMetrics.saveRocCurveAsImage(
									DATA_DIR + "svm_roc.jpg", true);
								binaryClassMetrics.saveRecallPrecisionCurveAsImage(
									DATA_DIR + "svm_recallprec.jpg", true);
								binaryClassMetrics.saveLiftChartAsImage(
									DATA_DIR + "svm_lift.jpg", true);
								binaryClassMetrics.saveKSAsImage(
									DATA_DIR + "svm_ks.jpg", true);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					})
			);

		BatchOperator.execute();

	}

	static void c_8() throws Exception {
		BatchOperator <?> train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		BatchOperator <?> test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		PipelineModel featureExpand = new Pipeline()
			.add(
				new VectorAssembler()
					.setSelectedCols(FEATURE_COL_NAMES)
					.setOutputCol(VEC_COL_NAME + "_0")
			)
			.add(
				new VectorPolynomialExpand()
					.setSelectedCol(VEC_COL_NAME + "_0")
					.setOutputCol(VEC_COL_NAME)
					.setDegree(2)
			)
			.fit(train_data);

		train_data = featureExpand.transform(train_data);
		test_data = featureExpand.transform(test_data);

		train_data.lazyPrint(1);

		new LinearSvm()
			.setVectorCol(VEC_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("LinearSVM")
			);

		new LogisticRegression()
			.setVectorCol(VEC_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("LogisticRegression")
			);

		new LogisticRegression()
			.setOptimMethod(OptimMethod.Newton)
			.setVectorCol(VEC_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("LogisticRegression + OptimMethod.Newton")
			);

		BatchOperator.execute();

	}

	static void c_9() throws Exception {
		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new FmClassifier()
			.setNumEpochs(10)
			.setLearnRate(0.5)
			.setNumFactor(2)
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.enableLazyPrintTrainInfo()
			.enableLazyPrintModelInfo()
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("FM")
			);

		BatchOperator.execute();

	}

}
