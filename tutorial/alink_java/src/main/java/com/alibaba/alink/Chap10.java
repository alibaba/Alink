package com.alibaba.alink;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelTrainInfo;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe.Encode;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.FeatureHasher;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.function.Consumer;

public class Chap10 {

	static final String DATA_DIR = Utils.ROOT_DIR + "german_credit" + File.separator;

	static final String ORIGIN_FILE = "german.data";

	static final String TRAIN_FILE = "train.ak";
	static final String TEST_FILE = "test.ak";

	private static final String[] COL_NAMES = new String[] {
		"status", "duration", "credit_history", "purpose", "credit_amount",
		"savings", "employment", "installment_rate", "marriage_sex", "debtors",
		"residence", "property", "age", "other_plan", "housing",
		"number_credits", "job", "maintenance_num", "telephone", "foreign_worker",
		"class"
	};

	private static final String[] COL_TYPES = new String[] {
		"string", "int", "string", "string", "int",
		"string", "string", "int", "string", "string",
		"int", "string", "int", "string", "string",
		"int", "string", "int", "string", "string",
		"int"
	};

	static final String CLAUSE_CREATE_FEATURES
		= "(case status when 'A11' then 1 else 0 end) as status_A11,"
		+ "(case status when 'A12' then 1 else 0 end) as status_A12,"
		+ "(case status when 'A13' then 1 else 0 end) as status_A13,"
		+ "(case status when 'A14' then 1 else 0 end) as status_A14,"
		+ "duration,"
		+ "(case credit_history when 'A30' then 1 else 0 end) as credit_history_A30,"
		+ "(case credit_history when 'A31' then 1 else 0 end) as credit_history_A31,"
		+ "(case credit_history when 'A32' then 1 else 0 end) as credit_history_A32,"
		+ "(case credit_history when 'A33' then 1 else 0 end) as credit_history_A33,"
		+ "(case credit_history when 'A34' then 1 else 0 end) as credit_history_A34,"
		+ "(case purpose when 'A40' then 1 else 0 end) as purpose_A40,"
		+ "(case purpose when 'A41' then 1 else 0 end) as purpose_A41,"
		+ "(case purpose when 'A42' then 1 else 0 end) as purpose_A42,"
		+ "(case purpose when 'A43' then 1 else 0 end) as purpose_A43,"
		+ "(case purpose when 'A44' then 1 else 0 end) as purpose_A44,"
		+ "(case purpose when 'A45' then 1 else 0 end) as purpose_A45,"
		+ "(case purpose when 'A46' then 1 else 0 end) as purpose_A46,"
		+ "(case purpose when 'A47' then 1 else 0 end) as purpose_A47,"
		+ "(case purpose when 'A48' then 1 else 0 end) as purpose_A48,"
		+ "(case purpose when 'A49' then 1 else 0 end) as purpose_A49,"
		+ "(case purpose when 'A410' then 1 else 0 end) as purpose_A410,"
		+ "credit_amount,"
		+ "(case savings when 'A61' then 1 else 0 end) as savings_A61,"
		+ "(case savings when 'A62' then 1 else 0 end) as savings_A62,"
		+ "(case savings when 'A63' then 1 else 0 end) as savings_A63,"
		+ "(case savings when 'A64' then 1 else 0 end) as savings_A64,"
		+ "(case savings when 'A65' then 1 else 0 end) as savings_A65,"
		+ "(case employment when 'A71' then 1 else 0 end) as employment_A71,"
		+ "(case employment when 'A72' then 1 else 0 end) as employment_A72,"
		+ "(case employment when 'A73' then 1 else 0 end) as employment_A73,"
		+ "(case employment when 'A74' then 1 else 0 end) as employment_A74,"
		+ "(case employment when 'A75' then 1 else 0 end) as employment_A75,"
		+ "installment_rate,"
		+ "(case marriage_sex when 'A91' then 1 else 0 end) as marriage_sex_A91,"
		+ "(case marriage_sex when 'A92' then 1 else 0 end) as marriage_sex_A92,"
		+ "(case marriage_sex when 'A93' then 1 else 0 end) as marriage_sex_A93,"
		+ "(case marriage_sex when 'A94' then 1 else 0 end) as marriage_sex_A94,"
		+ "(case marriage_sex when 'A95' then 1 else 0 end) as marriage_sex_A95,"
		+ "(case debtors when 'A101' then 1 else 0 end) as debtors_A101,"
		+ "(case debtors when 'A102' then 1 else 0 end) as debtors_A102,"
		+ "(case debtors when 'A103' then 1 else 0 end) as debtors_A103,"
		+ "residence,"
		+ "(case property when 'A121' then 1 else 0 end) as property_A121,"
		+ "(case property when 'A122' then 1 else 0 end) as property_A122,"
		+ "(case property when 'A123' then 1 else 0 end) as property_A123,"
		+ "(case property when 'A124' then 1 else 0 end) as property_A124,"
		+ "age,"
		+ "(case other_plan when 'A141' then 1 else 0 end) as other_plan_A141,"
		+ "(case other_plan when 'A142' then 1 else 0 end) as other_plan_A142,"
		+ "(case other_plan when 'A143' then 1 else 0 end) as other_plan_A143,"
		+ "(case housing when 'A151' then 1 else 0 end) as housing_A151,"
		+ "(case housing when 'A152' then 1 else 0 end) as housing_A152,"
		+ "(case housing when 'A153' then 1 else 0 end) as housing_A153,"
		+ "number_credits,"
		+ "(case job when 'A171' then 1 else 0 end) as job_A171,"
		+ "(case job when 'A172' then 1 else 0 end) as job_A172,"
		+ "(case job when 'A173' then 1 else 0 end) as job_A173,"
		+ "(case job when 'A174' then 1 else 0 end) as job_A174,"
		+ "maintenance_num,"
		+ "(case telephone when 'A192' then 1 else 0 end) as telephone,"
		+ "(case foreign_worker when 'A201' then 1 else 0 end) as foreign_worker,"
		+ "class ";

	static String LABEL_COL_NAME = "class";

	static String[] FEATURE_COL_NAMES = ArrayUtils.removeElements(COL_NAMES, new String[] {LABEL_COL_NAME});

	static final String[] NUMERIC_FEATURE_COL_NAMES = new String[] {
		"duration", "credit_amount", "installment_rate", "residence", "age", "number_credits", "maintenance_num"
	};

	static final String[] CATEGORY_FEATURE_COL_NAMES =
		ArrayUtils.removeElements(FEATURE_COL_NAMES, NUMERIC_FEATURE_COL_NAMES);

	static String VEC_COL_NAME = "vec";

	static String PREDICTION_COL_NAME = "pred";

	static String PRED_DETAIL_COL_NAME = "predinfo";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_0();

		c_1();

		c_2();

		c_3_1();

		c_3_2();

	}

	static void c_0() throws Exception {
		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setFilePath(DATA_DIR + ORIGIN_FILE)
			.setSchemaStr(Utils.generateSchemaString(COL_NAMES, COL_TYPES))
			.setFieldDelimiter(" ");

		source
			.lazyPrint(5, "< origin data >")
			.lazyPrintStatistics();

		BatchOperator.execute();

		Utils.splitTrainTestIfNotExist(source, DATA_DIR + TRAIN_FILE, DATA_DIR + TEST_FILE, 0.8);
	}

	static void c_1() throws Exception {
		BatchOperator <?> train_data =
			new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE).select(CLAUSE_CREATE_FEATURES);

		BatchOperator <?> test_data =
			new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE).select(CLAUSE_CREATE_FEATURES);

		String[] new_features = ArrayUtils.removeElement(train_data.getColNames(), LABEL_COL_NAME);

		train_data.lazyPrint(5, "< new features >");

		LogisticRegressionTrainBatchOp trainer = new LogisticRegressionTrainBatchOp()
			.setFeatureCols(new_features)
			.setLabelCol(LABEL_COL_NAME);

		LogisticRegressionPredictBatchOp predictor = new LogisticRegressionPredictBatchOp()
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(trainer);

		predictor.linkFrom(trainer, test_data);

		trainer
			.lazyPrintTrainInfo()
			.lazyCollectTrainInfo(new Consumer <LinearModelTrainInfo>() {
									  @Override
									  public void accept(LinearModelTrainInfo linearModelTrainInfo) {
										  printImportance(
											  linearModelTrainInfo.getColNames(),
											  linearModelTrainInfo.getImportance()
										  );
									  }
								  }
			);

		predictor.link(
			new EvalBinaryClassBatchOp()
				.setPositiveLabelValueString("2")
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
				.lazyPrintMetrics()
		);

		BatchOperator.execute();

	}

	public static void printImportance(String[] colNames, double[] importance) {
		ArrayList <Tuple2 <String, Double>> list = new ArrayList <>();
		for (int i = 0; i < colNames.length; i++) {
			list.add(Tuple2.of(colNames[i], importance[i]));
		}
		Collections.sort(list, new Comparator <Tuple2 <String, Double>>() {
			@Override
			public int compare(Tuple2 <String, Double> o1, Tuple2 <String, Double> o2) {
				return -(o1.f1).compareTo(o2.f1);
			}
		});
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < list.size(); i++) {
			sbd.append(i + 1).append(" \t")
				.append(list.get(i).f0).append(" \t")
				.append(list.get(i).f1).append("\n");
		}
		System.out.print(sbd.toString());
	}

	static void c_2() throws Exception {
		BatchOperator <?> train_data =
			new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE).select(CLAUSE_CREATE_FEATURES);

		BatchOperator <?> test_data =
			new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE).select(CLAUSE_CREATE_FEATURES);

		String[] new_features = ArrayUtils.removeElement(train_data.getColNames(), LABEL_COL_NAME);

		train_data.lazyPrint(5, "< new features >");

		LogisticRegressionTrainBatchOp trainer = new LogisticRegressionTrainBatchOp()
			.setFeatureCols(new_features)
			.setLabelCol(LABEL_COL_NAME)
			.setL1(0.01);

		LogisticRegressionPredictBatchOp predictor = new LogisticRegressionPredictBatchOp()
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(trainer);

		predictor.linkFrom(trainer, test_data);

		trainer
			.lazyPrintTrainInfo()
			.lazyCollectTrainInfo(
				new Consumer <LinearModelTrainInfo>() {
					@Override
					public void accept(LinearModelTrainInfo linearModelTrainInfo) {
						printImportance(
							linearModelTrainInfo.getColNames(),
							linearModelTrainInfo.getImportance()
						);
					}
				}
			);

		predictor.link(
			new EvalBinaryClassBatchOp()
				.setPositiveLabelValueString("2")
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
				.lazyPrintMetrics()
		);

		BatchOperator.execute();

	}

	static void c_3_1() throws Exception {
		BatchOperator <?> train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		BatchOperator <?> test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		Pipeline pipeline = new Pipeline()
			.add(
				new OneHotEncoder()
					.setSelectedCols(CATEGORY_FEATURE_COL_NAMES)
					.setEncode(Encode.VECTOR)
			)
			.add(
				new VectorAssembler()
					.setSelectedCols(FEATURE_COL_NAMES)
					.setOutputCol(VEC_COL_NAME)
			)
			.add(
				new LogisticRegression()
					.setVectorCol(VEC_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			);

		pipeline
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("2")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics()
			);

		BatchOperator.execute();

	}

	static void c_3_2() throws Exception {
		BatchOperator <?> train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		BatchOperator <?> test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		Pipeline pipeline = new Pipeline()
			.add(
				new FeatureHasher()
					.setSelectedCols(FEATURE_COL_NAMES)
					.setCategoricalCols(CATEGORY_FEATURE_COL_NAMES)
					.setOutputCol(VEC_COL_NAME)
			)
			.add(
				new LogisticRegression()
					.setVectorCol(VEC_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			);

		pipeline
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("2")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics()
			);

		BatchOperator.execute();

	}

}
