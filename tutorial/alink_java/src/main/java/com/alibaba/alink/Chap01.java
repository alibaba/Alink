package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.LinearRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.pipeline.sql.Select;

import java.io.File;

public class Chap01 {

	static final String DATA_DIR = Utils.ROOT_DIR + "temp" + File.separator;

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_5_1();

		c_5_2();

		c_5_3();

		c_5_4();

		c_5_5();

	}

	static void c_5_1() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath("http://archive.ics.uci.edu/ml/machine-learning-databases"
					+ "/iris/iris.data")
				.setSchemaStr("sepal_length double, sepal_width double, petal_length double, "
					+ "petal_width double, category string");

		source.firstN(5).print();

		source
			.sampleWithSize(10)
			.link(
				new CsvSinkBatchOp()
					.setFilePath(DATA_DIR + "iris_10.data")
					.setOverwriteSink(true)
			);

		BatchOperator.execute();
	}

	static void c_5_2() throws Exception {

		BatchOperator <?> train_set = new MemSourceBatchOp(
			new Row[] {
				Row.of(2009, 0.5),
				Row.of(2010, 9.36),
				Row.of(2011, 52.0),
				Row.of(2012, 191.0),
				Row.of(2013, 350.0),
				Row.of(2014, 571.0),
				Row.of(2015, 912.0),
				Row.of(2016, 1207.0),
				Row.of(2017, 1682.0),
			},
			new String[] {"x", "gmv"}
		);

		BatchOperator <?> pred_set
			= new MemSourceBatchOp(new Integer[] {2018, 2019}, "x");

		train_set = train_set.select("x, x*x AS x2, gmv");

		LinearRegTrainBatchOp trainer
			= new LinearRegTrainBatchOp()
			.setFeatureCols("x", "x2")
			.setLabelCol("gmv");

		train_set.link(trainer);

		trainer.link(
			new AkSinkBatchOp()
				.setFilePath(DATA_DIR + "gmv_reg.model")
				.setOverwriteSink(true)
		);

		BatchOperator.execute();

		BatchOperator <?> lr_model
			= new AkSourceBatchOp().setFilePath(DATA_DIR + "gmv_reg.model");

		pred_set = pred_set.select("x, x*x AS x2");

		LinearRegPredictBatchOp predictor
			= new LinearRegPredictBatchOp().setPredictionCol("pred");

		predictor
			.linkFrom(lr_model, pred_set)
			.print();

	}

	static void c_5_3() throws Exception {

		MemSourceStreamOp pred_set
			= new MemSourceStreamOp(new Integer[] {2018, 2019}, "x");

		BatchOperator <?> lr_model
			= new AkSourceBatchOp().setFilePath(DATA_DIR + "gmv_reg.model");

		LinearRegPredictStreamOp predictor
			= new LinearRegPredictStreamOp(lr_model).setPredictionCol("pred");

		pred_set
			.select("x, x*x AS x2")
			.link(predictor)
			.print();

		StreamOperator.execute();
	}

	static void c_5_4() throws Exception {

		MemSourceBatchOp train_set = new MemSourceBatchOp(
			new Row[] {
				Row.of(2009, 0.5),
				Row.of(2010, 9.36),
				Row.of(2011, 52.0),
				Row.of(2012, 191.0),
				Row.of(2013, 350.0),
				Row.of(2014, 571.0),
				Row.of(2015, 912.0),
				Row.of(2016, 1207.0),
				Row.of(2017, 1682.0),
			},
			new String[] {"x", "gmv"}
		);

		Pipeline pipeline = new Pipeline()
			.add(
				new Select()
					.setClause("*, x*x AS x2")
			)
			.add(
				new LinearRegression()
					.setFeatureCols("x", "x2")
					.setLabelCol("gmv")
					.setPredictionCol("pred")
			);

		File file = new File(DATA_DIR + "gmv_pipeline.model");
		if (file.exists()) {
			file.delete();
		}

		pipeline.fit(train_set).save(DATA_DIR + "gmv_pipeline.model");

		BatchOperator.execute();

		PipelineModel pipelineModel = PipelineModel.load(DATA_DIR + "gmv_pipeline.model");

		BatchOperator <?> pred_batch
			= new MemSourceBatchOp(new Integer[] {2018, 2019}, "x");

		pipelineModel
			.transform(pred_batch)
			.print();

		MemSourceStreamOp pred_stream
			= new MemSourceStreamOp(new Integer[] {2018, 2019}, "x");

		pipelineModel
			.transform(pred_stream)
			.print();

		StreamOperator.execute();

	}

	static void c_5_5() throws Exception {

		LocalPredictor predictor
			= new LocalPredictor(DATA_DIR + "gmv_pipeline.model", "x int");

		System.out.println(predictor.getOutputSchema());

		for (int x : new int[] {2018, 2019}) {
			System.out.println(predictor.map(Row.of(x)));
		}

	}

}
