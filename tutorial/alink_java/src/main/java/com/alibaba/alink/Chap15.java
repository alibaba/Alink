package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;

import java.io.File;

public class Chap15 {

	private static final String DATA_DIR = Utils.ROOT_DIR + "father_son" + File.separator;

	private static final String ORIGIN_FILE = "Pearson.txt";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setFilePath(DATA_DIR + ORIGIN_FILE)
			.setSchemaStr("father double, son double")
			.setFieldDelimiter("\t")
			.setIgnoreFirstLine(true);

		source.firstN(5).print();

		source.lazyPrintStatistics();

		source.filter("father>=71.5 AND father<72.5").lazyPrintStatistics("father 72");

		source.filter("father>=64.5 AND father<65.5").lazyPrintStatistics("father 65");

		LinearRegTrainBatchOp linear_model =
			new LinearRegTrainBatchOp()
				.setFeatureCols("father")
				.setLabelCol("son")
				.linkFrom(source);

		linear_model.lazyPrintTrainInfo();
		linear_model.lazyPrintModelInfo();

		LinearRegPredictBatchOp linear_reg =
			new LinearRegPredictBatchOp()
				.setPredictionCol("linear_reg")
				.linkFrom(linear_model, source);

		linear_reg.lazyPrint(5);

		BatchOperator.execute();

	}

}
