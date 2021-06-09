package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class LogisticRegressionMixVecTest extends AlinkTestBase {

	AlgoOperator getData() {

		Row[] array1 = new Row[] {
			Row.of(new Object[] {"$32$0:1.0 1:1.0 2:1.0 7:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1, "0:1.0 1:1.0 2:1.0 7:1.0"}),
			Row.of(new Object[] {"$32$0:1.0 1:1.0 2:0.0 7:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1, "0:1.0 1:1.0 2:0.0 7:1.0"}),
			Row.of(new Object[] {"$32$0:1.0 1:0.0 2:1.0 7:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1, "0:1.0 1:0.0 2:1.0 7:1.0"}),
			Row.of(new Object[] {"$32$0:1.0 1:0.0 2:1.0 7:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1, "0:1.0 1:0.0"}),
			Row.of(new Object[] {"$32$0:0.0 1:1.0 2:1.0 7:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0 1:1.0 2:1.0"}),
			Row.of(new Object[] {"$32$0:0.0 1:1.0 2:1.0 7:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0 1:1.0 2:1.0 7:0.0"}),
			Row.of(new Object[] {"$32$0:0.0 1:1.0 2:1.0 7:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0 1:1.0 2:1.0 7:0.0"}),
			Row.of(new Object[] {"$32$0:0.0", "1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0, "0:0.0"})
		};

		return new MemSourceBatchOp(
			Arrays.asList(array1), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels", "svec2"})
			.link(new Rebalance());

	}

	public class Rebalance extends BatchOperator <Rebalance> {
		@Override
		public Rebalance linkFrom(BatchOperator <?>... inputs) {

			DataSet <Row> ret = inputs[0].getDataSet().rebalance();
			setOutput(ret, inputs[0].getSchema());
			return null;
		}
	}

	@Test
	public void batchMixVecTest11() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

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

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest12() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

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

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest13() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

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

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest14() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new VectorAssembler()
				.setSelectedCols(new String[] {"svec", "vec", "f0", "f1", "f2", "f3"})
				.setOutputCol("allvec")
			);
			//.add(new LogisticRegression()
			//	.setVectorCol("allvec")
			//	.setWithIntercept(false)
			//	.setStandardization(true)
			//	.setReservedCols(new String[] {"labels", "allvec"})
			//	.setLabelCol("labels")
			//	.setPredictionCol("pred")
			//);

		PipelineModel model = pipeline.fit(trainData);

		BatchOperator result = model.transform(trainData);
		System.out.println(	result.getSchema());
		result.link(new AkSinkBatchOp().setOverwriteSink(true).setFilePath("/tmp/test_data.ak"));
		BatchOperator.execute();
	}

	@Test
	public void batchMixVecTest3() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec")
				.setWithIntercept(true)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest23() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec2")
				.setWithIntercept(true)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest4() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("vec")
				.setWithIntercept(true)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest5() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest15() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec2")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest6() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("vec")
				.setWithIntercept(false)
				.setStandardization(false)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest7() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec")
				.setWithIntercept(false)
				.setStandardization(true)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest17() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("svec2")
				.setWithIntercept(false)
				.setStandardization(true)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}

	@Test
	public void batchMixVecTest8() throws Exception {
		BatchOperator trainData = (BatchOperator) getData();

		Pipeline pipeline = new Pipeline()
			.add(new LogisticRegression()
				.setVectorCol("vec")
				.setWithIntercept(false)
				.setStandardization(true)
				.setLabelCol("labels")
				.setPredictionCol("pred"));

		PipelineModel model = pipeline.fit(trainData);

		model.transform(trainData).print();
	}
}
