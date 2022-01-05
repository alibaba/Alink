package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.KMeansPredictStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link KMeans}.
 */
public class KMeansTest extends AlinkTestBase {
	StreamOperator<?> inputStreamOp;
	BatchOperator<?> inputBatchOp;
	double[] expectedPrediction;

	@Before
	public void before() {
		Row[] rows = new Row[] {
			Row.of(0, "0  0  0"),
			Row.of(1, "0.1  0.1  0.1"),
			Row.of(2, "0.2  0.2  0.2"),
			Row.of(3, "9  9  9"),
			Row.of(4, "9.1  9.1  9.1"),
			Row.of(5, "9.2  9.2  9.2"),
		};
		inputBatchOp = new TableSourceBatchOp(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "vector"})
		);
		inputStreamOp = new TableSourceStreamOp(
			MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "vector"})
		);
		expectedPrediction = new double[] {0.173, 0, 0.173, 0.173, 0, 0.173};
	}

	@Test
	public void testKmeans() throws Exception {
		KMeans kMeans = new KMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setPredictionDistanceCol("distance")
			.setK(2);

		PipelineModel model = new Pipeline().add(kMeans).fit(inputBatchOp);
		BatchOperator <?> batchPredOp = model.transform(inputBatchOp)
			.select(new String[] {"id", "distance"});
		verifyPredResult(batchPredOp.collect());

		StreamOperator<?> streamPredOp = model.transform(inputStreamOp)
			.select(new String[] {"id", "distance"});
		CollectSinkStreamOp sinkOp = streamPredOp.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		verifyPredResult(sinkOp.getAndRemoveValues());
	}

	private void verifyPredResult(List <Row> res) {
		res.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
		double[] pred = res.stream().map(x -> (Double) x.getField(1)).mapToDouble(Double::doubleValue).toArray();
		assertArrayEquals(expectedPrediction, pred, 0.01);
	}

	@Test
	public void testInitializer() {
		KMeansModel model = new KMeansModel();
		assertEquals(model.getParams().size(), 0);
		KMeans kMeans = new KMeans(new Params());
		assertEquals(kMeans.getParams().size(), 0);

		KMeansTrainBatchOp op = new KMeansTrainBatchOp();
		assertEquals(op.getParams().size(), 0);

		KMeansPredictBatchOp predict = new KMeansPredictBatchOp(new Params());
		assertEquals(predict.getParams().size(), 0);
		predict = new KMeansPredictBatchOp();
		assertEquals(predict.getParams().size(), 0);

		KMeansPredictStreamOp predictStream = new KMeansPredictStreamOp(op, new Params());
		assertEquals(predictStream.getParams().size(), 0);
		predictStream = new KMeansPredictStreamOp(predict);
		assertEquals(predictStream.getParams().size(), 0);
	}

}