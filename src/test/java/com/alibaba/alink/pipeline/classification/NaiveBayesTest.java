package com.alibaba.alink.pipeline.classification;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class NaiveBayesTest {

	private static AlgoOperator getData(boolean isBatch) {
		Row[] array = new Row[] {
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
		};

		if (isBatch) {
			return new MemSourceBatchOp(
				Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
		} else {
			return new MemSourceStreamOp(
				Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
		}
	}

	@Test
	public void testPipelineBatch() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

		String[] featNames = new String[] {"f0", "f1", "f2", "f3"};
		String labelName = "labels";

        /* train model */
		NaiveBayes nb
			= new NaiveBayes()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setFeatureCols(featNames)
			.setPredictionCol("predResult")
			.setPredictionDetailCol("predResultColName")
			.setSmoothing(0.5);

		NaiveBayes vnb
			= new NaiveBayes()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setVectorCol("vec")
			.setPredictionCol("predvResult")
			.setPredictionDetailCol("predvResultColName")
			.setSmoothing(0.5);

		NaiveBayes svnb
			= new NaiveBayes()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setVectorCol("svec")
			.setPredictionCol("predsvResult")
			.setPredictionDetailCol("predsvResultColName")
			.setSmoothing(0.5);

		Pipeline pl = new Pipeline().add(nb).add(vnb).add(svnb);

		PipelineModel model = pl.fit((BatchOperator) getData(true));
		BatchOperator result = model.transform((BatchOperator) getData(true)).select(
			new String[] {"labels", "predResult", "predvResult", "predsvResult"});

		List<Row> data = result.collect();
		for (Row row : data) {
			for (int i = 1; i < 3; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}
		// below is stream test code.
		model.transform((StreamOperator) getData(false)).print();
		StreamOperator.execute();
	}

	@Test
	public void testBatch() throws Exception {
		String[] featNames = new String[] {"f0", "f1", "f2", "f3"};
		String labelName = "labels";
		NaiveBayesTrainBatchOp op = new NaiveBayesTrainBatchOp()
				.setModelType("Bernoulli")
				.setLabelCol(labelName)
				.setFeatureCols(featNames)
				.setSmoothing(0.5).linkFrom((BatchOperator) getData(true));
		NaiveBayesPredictBatchOp predict = new NaiveBayesPredictBatchOp().setPredictionCol("predsvResult")
				.setPredictionDetailCol("predsvResultColName");
		predict.linkFrom(op, (BatchOperator) getData(true)).print();
	}

	@Test
	public void testModelDataSerDeser() {
		NaiveBayesModelDataConverter.NaiveBayesProbInfo data = new NaiveBayesModelDataConverter.NaiveBayesProbInfo();
		data.piArray = new double[] { 1., 2., 3.};
		data.theta = DenseMatrix.eye(3);

		String str = JsonConverter.toJson(data);
		NaiveBayesModelDataConverter.NaiveBayesProbInfo obj = JsonConverter.fromJson(str, NaiveBayesModelDataConverter.NaiveBayesProbInfo.class);

		Assert.assertArrayEquals(data.piArray, obj.piArray, 0.0000001);
		Assert.assertArrayEquals(data.theta.getData(), obj.theta.getData(), 0.0000001);
		Assert.assertEquals(data.theta.numRows(), obj.theta.numRows());
		Assert.assertEquals(data.theta.numCols(), obj.theta.numCols());
	}
}
