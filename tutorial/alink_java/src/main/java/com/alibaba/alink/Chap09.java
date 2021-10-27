package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.C45TrainBatchOp;
import com.alibaba.alink.operator.batch.classification.DecisionTreePredictBatchOp;
import com.alibaba.alink.operator.batch.classification.DecisionTreeTrainBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesModelInfo;
import com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.feature.ChiSqSelectorBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.params.feature.BasedChisqSelectorParams.SelectorType;
import com.alibaba.alink.params.shared.tree.HasIndividualTreeType.TreeType;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Consumer;

public class Chap09 {

	static final String DATA_DIR = Utils.ROOT_DIR + "mushroom" + File.separator;

	static final String TEST_FILE = "test.ak";
	static final String TRAIN_FILE = "train.ak";
	static final String ORIGIN_FILE = "agaricus-lepiota.data";

	static final String[] COL_NAMES = new String[] {
		"class",
		"cap_shape", "cap_surface", "cap_color", "bruises", "odor",
		"gill_attachment", "gill_spacing", "gill_size", "gill_color",
		"stalk_shape", "stalk_root", "stalk_surface_above_ring", "stalk_surface_below_ring",
		"stalk_color_above_ring", "stalk_color_below_ring",
		"veil_type", "veil_color",
		"ring_number", "ring_type", "spore_print_color", "population", "habitat"
	};

	static final String[] COL_TYPES = new String[] {
		"string",
		"string", "string", "string", "string", "string",
		"string", "string", "string", "string", "string",
		"string", "string", "string", "string", "string",
		"string", "string", "string", "string", "string",
		"string", "string"
	};

	static final String LABEL_COL_NAME = "class";
	static final String[] FEATURE_COL_NAMES = ArrayUtils.removeElement(COL_NAMES, LABEL_COL_NAME);

	static final String PREDICTION_COL_NAME = "pred";
	static final String PRED_DETAIL_COL_NAME = "predInfo";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		//c_2_5();

		//c_3();
		//
		//c_4_a();
		//
		//c_4_b();
		//
		c_5();

	}

	static void c_2_5() throws Exception {
		MemSourceBatchOp source = new MemSourceBatchOp(
			new Row[] {
				Row.of("sunny", 85.0, 85.0, false, "no"),
				Row.of("sunny", 80.0, 90.0, true, "no"),
				Row.of("overcast", 83.0, 78.0, false, "yes"),
				Row.of("rainy", 70.0, 96.0, false, "yes"),
				Row.of("rainy", 68.0, 80.0, false, "yes"),
				Row.of("rainy", 65.0, 70.0, true, "no"),
				Row.of("overcast", 64.0, 65.0, true, "yes"),
				Row.of("sunny", 72.0, 95.0, false, "no"),
				Row.of("sunny", 69.0, 70.0, false, "yes"),
				Row.of("rainy", 75.0, 80.0, false, "yes"),
				Row.of("sunny", 75.0, 70.0, true, "yes"),
				Row.of("overcast", 72.0, 90.0, true, "yes"),
				Row.of("overcast", 81.0, 75.0, false, "yes"),
				Row.of("rainy", 71.0, 80.0, true, "no")
			},
			new String[] {"Outlook", "Temperature", "Humidity", "Windy", "Play"}
		);

		source.lazyPrint(-1);

		source
			.link(
				new C45TrainBatchOp()
					.setFeatureCols("Outlook", "Temperature", "Humidity", "Windy")
					.setCategoricalCols("Outlook", "Windy")
					.setLabelCol("Play")
					.lazyPrintModelInfo()
					.lazyCollectModelInfo(new Consumer <DecisionTreeModelInfo>() {
						@Override
						public void accept(DecisionTreeModelInfo decisionTreeModelInfo) {
							try {
								decisionTreeModelInfo.saveTreeAsImage(
									DATA_DIR + "weather_tree_model.png", true);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					})
			);

		BatchOperator.execute();
	}

	static void c_3() throws Exception {

		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setFilePath(DATA_DIR + ORIGIN_FILE)
			.setSchemaStr(Utils.generateSchemaString(COL_NAMES, COL_TYPES));

		source.lazyPrint(5, "< origin data >");

		Utils.splitTrainTestIfNotExist(source, DATA_DIR + TRAIN_FILE, DATA_DIR + TEST_FILE, 0.9);

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + TRAIN_FILE)
			.link(
				new ChiSqSelectorBatchOp()
					.setSelectorType(SelectorType.NumTopFeatures)
					.setNumTopFeatures(3)
					.setSelectedCols(FEATURE_COL_NAMES)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintModelInfo("< Chi-Square Selector >")
			);

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + TRAIN_FILE)
			.select("veil_type")
			.distinct()
			.lazyPrint(100);

		BatchOperator.execute();

	}

	static void c_4_a() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		NaiveBayesTrainBatchOp trainer =
			new NaiveBayesTrainBatchOp()
				.setFeatureCols(FEATURE_COL_NAMES)
				.setCategoricalCols(FEATURE_COL_NAMES)
				.setLabelCol(LABEL_COL_NAME);

		NaiveBayesPredictBatchOp predictor = new NaiveBayesPredictBatchOp()
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(trainer);
		predictor.linkFrom(trainer, test_data);

		trainer.lazyPrintModelInfo();

		trainer.lazyCollectModelInfo(new Consumer <NaiveBayesModelInfo>() {
			@Override
			public void accept(NaiveBayesModelInfo naiveBayesModelInfo) {
				StringBuilder sbd = new StringBuilder();
				for (String feature : new String[] {"odor", "spore_print_color", "gill_color"}) {
					HashMap <Object, HashMap <Object, Double>> map2 =
						naiveBayesModelInfo.getCategoryFeatureInfo().get(feature);
					sbd.append("\nfeature:").append(feature);
					for (Entry <Object, HashMap <Object, Double>> entry : map2.entrySet()) {
						sbd.append("\n").append(entry.getKey()).append(" : ")
							.append(entry.getValue().toString());
					}
				}
				System.out.println(sbd.toString());
			}
		});

		predictor.lazyPrint(10, "< Prediction >");

		predictor
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("p")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics()
			);

		BatchOperator.execute();

	}

	static void c_4_b() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		NaiveBayesTrainBatchOp trainer =
			new NaiveBayesTrainBatchOp()
				.setFeatureCols("odor", "gill_color")
				.setCategoricalCols("odor", "gill_color")
				.setLabelCol(LABEL_COL_NAME);

		NaiveBayesPredictBatchOp predictor = new NaiveBayesPredictBatchOp()
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(trainer);
		predictor.linkFrom(trainer, test_data);

		trainer.lazyCollectModelInfo(new Consumer <NaiveBayesModelInfo>() {
			@Override
			public void accept(NaiveBayesModelInfo naiveBayesModelInfo) {
				StringBuilder sbd = new StringBuilder();
				for (String feature : new String[] {"odor", "gill_color"}) {
					HashMap <Object, HashMap <Object, Double>> map2 =
						naiveBayesModelInfo.getCategoryFeatureInfo().get(feature);
					sbd.append("\nfeature:").append(feature);
					for (Entry <Object, HashMap <Object, Double>> entry : map2.entrySet()) {
						sbd.append("\n").append(entry.getKey()).append(" : ")
							.append(entry.getValue().toString());
					}
				}
				System.out.println(sbd.toString());
			}
		});

		predictor
			.lazyPrint(10, "< Prediction >")
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("p")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics()
			);

		BatchOperator.execute();
	}


	static void c_5() throws Exception {

		BatchOperator train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		BatchOperator test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		for (TreeType treeType : new TreeType[] {TreeType.GINI, TreeType.INFOGAIN, TreeType.INFOGAINRATIO}) {
			BatchOperator <?> model = train_data
				.link(
					new DecisionTreeTrainBatchOp()
						.setTreeType(treeType)
						.setFeatureCols(FEATURE_COL_NAMES)
						.setCategoricalCols(FEATURE_COL_NAMES)
						.setLabelCol(LABEL_COL_NAME)
						.lazyPrintModelInfo("< " + treeType.toString() + " >")
						.lazyCollectModelInfo(new Consumer <DecisionTreeModelInfo>() {
							@Override
							public void accept(DecisionTreeModelInfo decisionTreeModelInfo) {
								try {
									decisionTreeModelInfo.saveTreeAsImage(
										DATA_DIR + "tree_" + treeType.toString() + ".jpg", true);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						})
				);

			DecisionTreePredictBatchOp predictor = new DecisionTreePredictBatchOp()
				.setPredictionCol(PREDICTION_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

			predictor.linkFrom(model, test_data);

			predictor.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("p")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("< " + treeType.toString() + " >")
			);
		}

		BatchOperator.execute();

	}

}
