package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelDataConverter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextTest {

	private static AlgoOperator getData(boolean isBatch) {
		Row[] array = new Row[] {
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0),
		};

		if (isBatch) {
			return new MemSourceBatchOp(
				Arrays.asList(array), new String[] {"svec", "vec", "labels"});
		} else {
			return new MemSourceStreamOp(
				Arrays.asList(array), new String[] {"svec", "vec", "labels"});
		}
	}

	@Test
	public void testPipelineBatch() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

		String labelName = "labels";


		NaiveBayesTextClassifier vnb
			= new NaiveBayesTextClassifier()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setVectorCol("vec")
			.setPredictionCol("predvResult")
			.setPredictionDetailCol("predvResultColName")
			.setSmoothing(0.5);

		NaiveBayesTextClassifier svnb
			= new NaiveBayesTextClassifier()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setVectorCol("svec")
			.setPredictionCol("predsvResult")
			.setPredictionDetailCol("predsvResultColName")
			.setSmoothing(0.5);

		Pipeline pl = new Pipeline().add(vnb).add(svnb);

		PipelineModel model = pl.fit((BatchOperator) getData(true));
		BatchOperator result = model.transform((BatchOperator) getData(true)).select(
			new String[] {"labels", "predvResult", "predsvResult"});

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
		String labelName = "labels";
		NaiveBayesTextTrainBatchOp op = new NaiveBayesTextTrainBatchOp()
				.setModelType("Bernoulli")
				.setLabelCol(labelName)
				.setVectorCol("vec")
				.setSmoothing(0.5).linkFrom((BatchOperator) getData(true));
		NaiveBayesTextPredictBatchOp predict = new NaiveBayesTextPredictBatchOp()
				.setPredictionCol("predsvResult")
				.setVectorCol("vec")
				.setPredictionDetailCol("predsvResultColName");
		predict.linkFrom(op, (BatchOperator) getData(true)).print();
	}

	@Test
	public void testModelDataSerDeser() {
		NaiveBayesTextModelDataConverter.NaiveBayesTextProbInfo data = new NaiveBayesTextModelDataConverter.NaiveBayesTextProbInfo();
		data.piArray = new double[] { 1., 2., 3.};
		data.theta = DenseMatrix.eye(3);

		String str = JsonConverter.toJson(data);
		NaiveBayesTextModelDataConverter.NaiveBayesTextProbInfo obj =
				JsonConverter.fromJson(str, NaiveBayesTextModelDataConverter.NaiveBayesTextProbInfo.class);

		Assert.assertArrayEquals(data.piArray, obj.piArray, 0.0000001);
		Assert.assertArrayEquals(data.theta.getData(), obj.theta.getData(), 0.0000001);
		Assert.assertEquals(data.theta.numRows(), obj.theta.numRows());
		Assert.assertEquals(data.theta.numCols(), obj.theta.numCols());
	}
}
