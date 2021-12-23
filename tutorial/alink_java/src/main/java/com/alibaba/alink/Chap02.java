package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.C45TrainBatchOp;
import com.alibaba.alink.operator.batch.classification.DecisionTreeModelInfoBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegModelInfoBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.pipeline.regression.LinearRegressionModel;
import com.alibaba.alink.pipeline.sql.Select;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class Chap02 {

	static final String DATA_DIR = Utils.ROOT_DIR + "temp" + File.separator;

	static final String TREE_MODEL_FILE = "tree_model.ak";
	static final String PIPELINE_MODEL_FILE = "pipeline_model.ak";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_6();

	}

	static void c_6() throws Exception {
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
			new String[] {"outlook", "Temperature", "Humidity", "Windy", "play"}
		);

		source
			.link(
				new C45TrainBatchOp()
					.setFeatureCols("outlook", "Temperature", "Humidity", "Windy")
					.setCategoricalCols("outlook", "Windy")
					.setLabelCol("play")
			)
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + TREE_MODEL_FILE)
					.setOverwriteSink(true)
			);

		BatchOperator.execute();

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + TREE_MODEL_FILE)
			.link(
				new DecisionTreeModelInfoBatchOp()
					.lazyPrintModelInfo()
					.lazyCollectModelInfo(new Consumer <DecisionTreeModelInfo>() {
						@Override
						public void accept(DecisionTreeModelInfo decisionTreeModelInfo) {
							try {
								decisionTreeModelInfo.saveTreeAsImage(
									DATA_DIR + "tree_model.png", true);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					})
			);
		BatchOperator.execute();

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
		pipeline.fit(train_set).save(DATA_DIR + PIPELINE_MODEL_FILE, true);
		BatchOperator.execute();

		PipelineModel pipelineModel = PipelineModel.load(DATA_DIR + PIPELINE_MODEL_FILE);

		TransformerBase <?>[] stages = pipelineModel.getTransformers();

		for (int i = 0; i < stages.length; i++) {
			System.out.println(String.valueOf(i) + "\t" + stages[i]);
		}

		((LinearRegressionModel) stages[1]).getModelData()
			.link(
				new LinearRegModelInfoBatchOp()
					.lazyPrintModelInfo()
			);
		BatchOperator.execute();

	}

}
