package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class LogisticRegressionMixVecTest extends AlinkTestBase {

	AlgoOperator<?> getData() {

		Row[] array1 = new Row[] {
			Row.of("$32$0:1.0 1:1.0 2:1.0 7:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1, "0:1.0 1:1.0 2:1.0 7:1.0"),
			Row.of("$32$0:1.0 1:1.0 2:0.0 7:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1, "0:1.0 1:1.0 2:0.0 7:1.0"),
			Row.of("$32$0:1.0 1:0.0 2:1.0 7:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1, "0:1.0 1:0.0 2:1.0 7:1.0"),
			Row.of("$32$0:1.0 1:0.0 2:1.0 7:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1, "0:1.0 1:0.0"),
			Row.of("$32$0:0.0 1:1.0 2:1.0 7:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0 1:1.0 2:1.0"),
			Row.of("$32$0:0.0 1:1.0 2:1.0 7:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0 1:1.0 2:1.0 7:0.0"),
			Row.of("$32$0:0.0 1:1.0 2:1.0 7:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0 1:1.0 2:1.0 7:0.0"),
			Row.of("$32$0:0.0", "1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0")
		};

		return new MemSourceBatchOp(
			Arrays.asList(array1), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels", "svec2"})
			.link(new Rebalance());

	}

	public static class Rebalance extends BatchOperator<Rebalance> {
		@Override
		public Rebalance linkFrom(BatchOperator<?>... inputs) {

			DataSet <Row> ret = inputs[0].getDataSet().rebalance();
			setOutput(ret, inputs[0].getSchema());
			return null;
		}
	}

	@Test
	public void batchMixVecTest11() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new VectorAssembler()
				.setSelectedCols(new String[] {"svec", "vec", "f0", "f1", "f2", "f3"})
				.setOutputCol("allvec")
			).add(new LogisticRegression()
				.setVectorCol("allvec")
				.setWithIntercept(true)
				.setReservedCols(new String[]{"labels"})
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest12() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new VectorAssembler()
				.setSelectedCols(new String[] {"svec", "vec", "f0", "f1", "f2", "f3"})
				.setOutputCol("allvec")
			).add(new LogisticRegression()
				.setVectorCol("allvec")
				.setWithIntercept(true)
				.setReservedCols(new String[] {"labels", "allvec"})
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest13() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new VectorAssembler()
				.setSelectedCols(new String[] {"svec", "vec", "f0", "f1", "f2", "f3"})
				.setOutputCol("allvec")
			).add(new LogisticRegression()
				.setVectorCol("allvec")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setReservedCols(new String[] {"labels"})
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest14() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new VectorAssembler()
				.setSelectedCols(new String[] {"svec", "vec", "f0", "f1", "f2", "f3"})
				.setOutputCol("allvec")
			);

		PipelineModel model = pipeline.fit(trainData);

		BatchOperator<?> result = model.transform(trainData);
		result.collect();
	}

	@Test
	public void batchMixVecTest3() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec")
				.setWithIntercept(true)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest23() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec2")
				.setWithIntercept(true)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest4() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("vec")
				.setWithIntercept(true)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest5() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest15() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec2")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest6() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("vec")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest7() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec")
				.setWithIntercept(false)
				.setStandardization(true)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest17() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec2")
				.setWithIntercept(false)
				.setStandardization(true)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}

	@Test
	public void batchMixVecTest8() {
		BatchOperator<?> trainData = (BatchOperator<?>) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("vec")
				.setWithIntercept(false)
				.setStandardization(true)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).collect();
	}
}
