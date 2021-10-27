package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningBinaryClassMetric;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.NaiveBayesTextClassifier;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.nlp.DocCountVectorizer;
import com.alibaba.alink.pipeline.nlp.DocHashCountVectorizer;
import com.alibaba.alink.pipeline.nlp.NGram;
import com.alibaba.alink.pipeline.nlp.RegexTokenizer;
import com.alibaba.alink.pipeline.tuning.BinaryClassificationTuningEvaluator;
import com.alibaba.alink.pipeline.tuning.GridSearchCV;
import com.alibaba.alink.pipeline.tuning.GridSearchCVModel;
import com.alibaba.alink.pipeline.tuning.ParamGrid;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Chap23 {

	static String DATA_DIR = Utils.ROOT_DIR + "sentiment_imdb" + File.separator;
	static String ORIGIN_DATA_DIR = DATA_DIR + "aclImdb" + File.separator;

	static final String TRAIN_FILE = "train.ak";
	static final String TEST_FILE = "test.ak";

	static String PIPELINE_MODEL = "pipeline_model.ak";

	private static final String TXT_COL_NAME = "review";
	private static final String LABEL_COL_NAME = "label";
	private static final String VECTOR_COL_NAME = "vec";
	private static final String PREDICTION_COL_NAME = "pred";
	private static final String PRED_DETAIL_COL_NAME = "predinfo";

	static String[] COL_NAMES = new String[] {LABEL_COL_NAME, TXT_COL_NAME};

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_1();

		c_2();

		BatchOperator.setParallelism(4);

		c_3();

		c_4();

	}

	static void c_1() throws Exception {
		BatchOperator <?> train_set = new LibSvmSourceBatchOp()
			.setFilePath(ORIGIN_DATA_DIR + "train" + File.separator + "labeledBow.feat")
			.setStartIndex(0);

		train_set.lazyPrint(1, "train_set");

		train_set
			.groupBy("label", "label, COUNT(label) AS cnt")
			.orderBy("label", 100)
			.lazyPrint(-1, "labels of train_set");

		BatchOperator <?> test_set = new LibSvmSourceBatchOp()
			.setFilePath(ORIGIN_DATA_DIR + "test" + File.separator + "labeledBow.feat")
			.setStartIndex(0);

		train_set = train_set.select("CASE WHEN label>5 THEN 'pos' ELSE 'neg' END AS label, "
			+ "features AS " + VECTOR_COL_NAME);
		test_set = test_set.select("CASE WHEN label>5 THEN 'pos' ELSE 'neg' END AS label, "
			+ "features AS " + VECTOR_COL_NAME);

		train_set.lazyPrint(1, "train_set");

		new NaiveBayesTextClassifier()
			.setModelType("Multinomial")
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.enableLazyPrintModelInfo()
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("NaiveBayesTextClassifier + Multinomial")
			);
		BatchOperator.execute();

		new Pipeline()
			.add(
				new Binarizer()
					.setSelectedCol(VECTOR_COL_NAME)
					.enableLazyPrintTransformData(1, "After Binarizer")
			)
			.add(
				new NaiveBayesTextClassifier()
					.setModelType("Bernoulli")
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.enableLazyPrintModelInfo()
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("Binarizer + NaiveBayesTextClassifier + Bernoulli")
			);
		BatchOperator.execute();

		new LogisticRegression()
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.enableLazyPrintTrainInfo("< LR train info >")
			.enableLazyPrintModelInfo("< LR model info >")
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("LogisticRegression")
			);
		BatchOperator.execute();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		LogisticRegression lr = new LogisticRegression()
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		GridSearchCV gridSearch = new GridSearchCV()
			.setEstimator(
				new Pipeline().add(lr)
			)
			.setParamGrid(
				new ParamGrid()
					.addGrid(lr, LogisticRegression.MAX_ITER,
						new Integer[] {10, 20, 30, 40, 50, 60, 80, 100})
			)
			.setTuningEvaluator(
				new BinaryClassificationTuningEvaluator()
					.setLabelCol(LABEL_COL_NAME)
					.setPositiveLabelValueString("pos")
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.setTuningBinaryClassMetric(TuningBinaryClassMetric.AUC)
			)
			.setNumFolds(6)
			.enableLazyPrintTrainInfo();

		GridSearchCVModel bestModel = gridSearch.fit(train_set);

		bestModel
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("LogisticRegression")
			);
		BatchOperator.execute();

	}

	private static String readFileContent(File f) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(f));
		StringBuilder sbd = new StringBuilder();
		String t = null;
		while ((t = reader.readLine()) != null) {
			sbd.append(t);
		}
		reader.close();
		return sbd.toString();

	}

	static void c_2() throws Exception {
		if (!new File(DATA_DIR + TRAIN_FILE).exists()) {
			ArrayList <Row> trainRows = new ArrayList <>();
			ArrayList <Row> testRows = new ArrayList <>();

			for (String label : new String[] {"pos", "neg"}) {
				File subfolder = new File(ORIGIN_DATA_DIR + "train" + File.separator + label);
				for (File f : subfolder.listFiles()) {
					trainRows.add(Row.of(label, readFileContent(f)));
				}
			}
			for (String label : new String[] {"pos", "neg"}) {
				File subfolder = new File(ORIGIN_DATA_DIR + "test" + File.separator + label);
				for (File f : subfolder.listFiles()) {
					testRows.add(Row.of(label, readFileContent(f)));
				}
			}

			new MemSourceBatchOp(trainRows, COL_NAMES)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + TRAIN_FILE)
				);
			new MemSourceBatchOp(testRows, COL_NAMES)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + TEST_FILE)
				);
			BatchOperator.execute();
		}

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		train_set.lazyPrint(2);

		new Pipeline()
			.add(
				new RegexTokenizer()
					.setPattern("\\W+")
					.setSelectedCol(TXT_COL_NAME)
			)
			.add(
				new DocCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol(VECTOR_COL_NAME)
					.enableLazyPrintTransformData(1)
			)
			.add(
				new LogisticRegression()
					.setMaxIter(30)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("DocCountVectorizer")
			);
		BatchOperator.execute();

		new Pipeline()
			.add(
				new RegexTokenizer()
					.setPattern("\\W+")
					.setSelectedCol(TXT_COL_NAME)
			)
			.add(
				new DocHashCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol(VECTOR_COL_NAME)
					.enableLazyPrintTransformData(1)
			)
			.add(
				new LogisticRegression()
					.setMaxIter(30)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("DocHashCountVectorizer")
			);
		BatchOperator.execute();

	}

	static void c_3() throws Exception {
		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new Pipeline()
			.add(
				new RegexTokenizer()
					.setPattern("\\W+")
					.setSelectedCol(TXT_COL_NAME)
			)
			.add(
				new DocCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol(VECTOR_COL_NAME)
			)
			.add(
				new NGram()
					.setN(2)
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol("v_2")
					.enableLazyPrintTransformData(1, "2-gram")
			)
			.add(
				new DocCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setSelectedCol("v_2")
					.setOutputCol("v_2")
			)
			.add(
				new VectorAssembler()
					.setSelectedCols(VECTOR_COL_NAME, "v_2")
					.setOutputCol(VECTOR_COL_NAME)
			)
			.add(
				new LogisticRegression()
					.setMaxIter(30)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("NGram 2")
			);
		BatchOperator.execute();

		new Pipeline()
			.add(
				new RegexTokenizer()
					.setPattern("\\W+")
					.setSelectedCol(TXT_COL_NAME)
			)
			.add(
				new DocCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol(VECTOR_COL_NAME)
			)
			.add(
				new NGram()
					.setN(2)
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol("v_2")
			)
			.add(
				new DocCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setSelectedCol("v_2")
					.setOutputCol("v_2")
			)
			.add(
				new NGram()
					.setN(3)
					.setSelectedCol(TXT_COL_NAME)
					.setOutputCol("v_3")
			)
			.add(
				new DocCountVectorizer()
					.setFeatureType("WORD_COUNT")
					.setVocabSize(10000)
					.setSelectedCol("v_3")
					.setOutputCol("v_3")
			)
			.add(
				new VectorAssembler()
					.setSelectedCols(VECTOR_COL_NAME, "v_2", "v_3")
					.setOutputCol(VECTOR_COL_NAME)
			)
			.add(
				new LogisticRegression()
					.setMaxIter(30)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("NGram 2 and 3")
			);
		BatchOperator.execute();

	}

	static void c_4() throws Exception {
		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);

		if (!new File(DATA_DIR + PIPELINE_MODEL).exists()) {
			new Pipeline()
				.add(
					new RegexTokenizer()
						.setPattern("\\W+")
						.setSelectedCol(TXT_COL_NAME)
				)
				.add(
					new DocCountVectorizer()
						.setFeatureType("WORD_COUNT")
						.setSelectedCol(TXT_COL_NAME)
						.setOutputCol(VECTOR_COL_NAME)
				)
				.add(
					new NGram()
						.setN(2)
						.setSelectedCol(TXT_COL_NAME)
						.setOutputCol("v_2")
				)
				.add(
					new DocCountVectorizer()
						.setFeatureType("WORD_COUNT")
						.setVocabSize(50000)
						.setSelectedCol("v_2")
						.setOutputCol("v_2")
				)
				.add(
					new NGram()
						.setN(3)
						.setSelectedCol(TXT_COL_NAME)
						.setOutputCol("v_3")
				)
				.add(
					new DocCountVectorizer()
						.setFeatureType("WORD_COUNT")
						.setVocabSize(10000)
						.setSelectedCol("v_3")
						.setOutputCol("v_3")
				)
				.add(
					new VectorAssembler()
						.setSelectedCols(VECTOR_COL_NAME, "v_2", "v_3")
						.setOutputCol(VECTOR_COL_NAME)
				)
				.add(
					new LogisticRegression()
						.setMaxIter(30)
						.setVectorCol(VECTOR_COL_NAME)
						.setLabelCol(LABEL_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
				)
				.fit(train_set)
				.save(DATA_DIR + PIPELINE_MODEL);
			BatchOperator.execute();
		}

		PipelineModel pipeline_model = PipelineModel.load(DATA_DIR + PIPELINE_MODEL);

		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);
		pipeline_model
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("pos")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("NGram 2 and 3")
			);
		BatchOperator.execute();

		AkSourceStreamOp test_stream = new AkSourceStreamOp().setFilePath(DATA_DIR + TEST_FILE);
		pipeline_model
			.transform(test_stream)
			.sample(0.001)
			.select(PREDICTION_COL_NAME + ", " + LABEL_COL_NAME + ", " + TXT_COL_NAME)
			.print();
		StreamOperator.execute();

		String str
			= "Oh dear. good cast, but to write and direct is an art and to write wit and direct wit is a bit of a "
			+ "task. Even doing good comedy you have to get the timing and moment right. Im not putting it all down "
			+ "there were parts where i laughed loud but that was at very few times. The main focus to me was on the "
			+ "fast free flowing dialogue, that made some people in the film annoying. It may sound great while "
			+ "reading the script in your head but getting that out and to the camera is a different task. And the "
			+ "hand held camera work does give energy to few parts of the film. Overall direction was good but the "
			+ "script was not all that to me, but I'm sure you was reading the script in your head it would sound good"
			+ ". Sorry.";

		Row pred_row;

		LocalPredictor local_predictor = pipeline_model.collectLocalPredictor("review string");

		System.out.println(local_predictor.getOutputSchema());

		pred_row = local_predictor.map(Row.of(str));

		System.out.println(pred_row.getField(4));

		LocalPredictor local_predictor_2
			= new LocalPredictor(DATA_DIR + PIPELINE_MODEL, "review string");

		System.out.println(local_predictor_2.getOutputSchema());

		pred_row = local_predictor_2.map(Row.of(str));

		System.out.println(pred_row.getField(4));

	}

}
