package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.dataproc.StratifiedSampleBatchOp;
import com.alibaba.alink.operator.batch.dataproc.WeightSampleBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorNormalizeBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.StratifiedSampleStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.params.dataproc.HasStrategy.Strategy;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.dataproc.Imputer;
import com.alibaba.alink.pipeline.dataproc.MaxAbsScaler;
import com.alibaba.alink.pipeline.dataproc.MinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMaxAbsScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScaler;

import java.io.File;

public class Chap07 {

	static final String DATA_DIR = Utils.ROOT_DIR + "iris" + File.separator;

	static final String ORIGIN_FILE = "iris.data";

	static final String TRAIN_FILE = "train.ak";
	static final String TEST_FILE = "test.ak";

	static final String SCHEMA_STRING
		= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

	static final String[] FEATURE_COL_NAMES
		= new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width"};

	static final String LABEL_COL_NAME = "category";

	static final String VECTOR_COL_NAME = "vec";

	static final String PREDICTION_COL_NAME = "pred";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_1_1();

		c_1_2();

		c_1_3();

		c_1_4();

		c_2();

		c_3_1();

		c_3_2();

		c_3_3();

		c_4_1();

		c_4_2();

		c_5();

	}

	static void c_1_1() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source
			.link(
				new FirstNBatchOp()
					.setSize(5)
			)
			.print();

		source.firstN(5).print();

	}

	static void c_1_2() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source
			.sampleWithSize(50)
			.lazyPrintStatistics("< after sample with size 50 >")
			.sample(0.1)
			.print();

		source
			.lazyPrintStatistics("< origin data >")
			.sampleWithSize(150, true)
			.lazyPrintStatistics("< after sample with size 150 >")
			.sample(0.03, true)
			.print();

		CsvSourceStreamOp source_stream =
			new CsvSourceStreamOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source_stream.sample(0.1).print();

		StreamOperator.execute();

	}

	static void c_1_3() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source
			.select("*, CASE category WHEN 'Iris-versicolor' THEN 1 "
				+ "WHEN 'Iris-setosa' THEN 2 ELSE 4 END AS weight")
			.link(
				new WeightSampleBatchOp()
					.setRatio(0.4)
					.setWeightCol("weight")
			)
			.groupBy("category", "category, COUNT(*) AS cnt")
			.print();

	}

	static void c_1_4() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source
			.link(
				new StratifiedSampleBatchOp()
					.setStrataCol("category")
					.setStrataRatios("Iris-versicolor:0.2,Iris-setosa:0.4,Iris-virginica:0.8")
			)
			.groupBy("category", "category, COUNT(*) AS cnt")
			.print();

		CsvSourceStreamOp source_stream =
			new CsvSourceStreamOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source_stream
			.link(
				new StratifiedSampleStreamOp()
					.setStrataCol("category")
					.setStrataRatios("Iris-versicolor:0.2,Iris-setosa:0.4,Iris-virginica:0.8")
			)
			.print();

		StreamOperator.execute();

	}

	static void c_2() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		System.out.println("schema of source:");
		System.out.println(source.getSchema());

		SplitBatchOp spliter = new SplitBatchOp().setFraction(0.9);

		source.link(spliter);

		System.out.println("schema of spliter's main output:");
		System.out.println(spliter.getSchema());

		System.out.println("count of spliter's side outputs:");
		System.out.println(spliter.getSideOutputCount());

		System.out.println("schema of spliter's side output :");
		System.out.println(spliter.getSideOutput(0).getSchema());

		spliter
			.lazyPrintStatistics("< Main Output >")
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + TRAIN_FILE)
					.setOverwriteSink(true)
			);

		spliter.getSideOutput(0)
			.lazyPrintStatistics("< Side Output >")
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + TEST_FILE)
					.setOverwriteSink(true)
			);

		BatchOperator.execute();

	}

	static void c_3_1() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source.lazyPrintStatistics("< Origin data >");

		StandardScaler scaler = new StandardScaler().setSelectedCols(FEATURE_COL_NAMES);

		scaler
			.fit(source)
			.transform(source)
			.lazyPrintStatistics("< after Standard Scale >");

		BatchOperator.execute();
	}

	static void c_3_2() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source.lazyPrintStatistics("< Origin data >");

		MinMaxScaler scaler = new MinMaxScaler().setSelectedCols(FEATURE_COL_NAMES);

		scaler
			.fit(source)
			.transform(source)
			.lazyPrintStatistics("< after MinMax Scale >");

		BatchOperator.execute();
	}

	static void c_3_3() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source.lazyPrintStatistics("< Origin data >");

		MaxAbsScaler scaler = new MaxAbsScaler().setSelectedCols(FEATURE_COL_NAMES);

		scaler
			.fit(source)
			.transform(source)
			.lazyPrintStatistics("< after MaxAbs Scale >");

		BatchOperator.execute();
	}

	static void c_4_1() throws Exception {
		BatchOperator <?> source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING)
				.link(
					new VectorAssemblerBatchOp()
						.setSelectedCols(FEATURE_COL_NAMES)
						.setOutputCol(VECTOR_COL_NAME)
						.setReservedCols(LABEL_COL_NAME)
				);

		source.link(
			new VectorSummarizerBatchOp()
				.setSelectedCol(VECTOR_COL_NAME)
				.lazyPrintVectorSummary("< Origin data >")
		);

		new VectorStandardScaler()
			.setSelectedCol(VECTOR_COL_NAME)
			.fit(source)
			.transform(source)
			.link(
				new VectorSummarizerBatchOp()
					.setSelectedCol(VECTOR_COL_NAME)
					.lazyPrintVectorSummary("< after Vector Standard Scale >")
			);

		new VectorMinMaxScaler()
			.setSelectedCol(VECTOR_COL_NAME)
			.fit(source)
			.transform(source)
			.link(
				new VectorSummarizerBatchOp()
					.setSelectedCol(VECTOR_COL_NAME)
					.lazyPrintVectorSummary("< after Vector MinMax Scale >")
			);

		new VectorMaxAbsScaler()
			.setSelectedCol(VECTOR_COL_NAME)
			.fit(source)
			.transform(source)
			.link(
				new VectorSummarizerBatchOp()
					.setSelectedCol(VECTOR_COL_NAME)
					.lazyPrintVectorSummary("< after Vector MaxAbs Scale >")
			);

		BatchOperator.execute();
	}

	static void c_4_2() throws Exception {
		BatchOperator <?> source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING)
				.link(
					new VectorAssemblerBatchOp()
						.setSelectedCols(FEATURE_COL_NAMES)
						.setOutputCol(VECTOR_COL_NAME)
						.setReservedCols(LABEL_COL_NAME)
				);

		source
			.link(
				new VectorNormalizeBatchOp()
					.setSelectedCol(VECTOR_COL_NAME)
					.setP(1.0)
			)
			.firstN(5)
			.print();
	}

	static void c_5() throws Exception {

		Row[] rows = new Row[] {
			Row.of("a", 10.0, 100),
			Row.of("b", -2.5, 9),
			Row.of("c", 100.2, 1),
			Row.of("d", -99.9, 100),
			Row.of(null, null, null)
		};

		MemSourceBatchOp source
			= new MemSourceBatchOp(rows, new String[] {"col1", "col2", "col3"});

		source.lazyPrint(-1, "< origin data >");

		Pipeline pipeline = new Pipeline()
			.add(
				new Imputer()
				.setSelectedCols("col1")
				.setStrategy(Strategy.VALUE)
				.setFillValue("e")
			)
			.add(
				new Imputer()
				.setSelectedCols("col2", "col3")
				.setStrategy(Strategy.MEAN)
			);

		pipeline.fit(source)
			.transform(source)
			.print();


		System.out.println(210/4);

	}

}
