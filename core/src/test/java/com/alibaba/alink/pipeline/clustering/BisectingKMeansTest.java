package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.BisectingKMeansPredictStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

/**
 * Tests the {@link BisectingKMeans}.
 */
public class BisectingKMeansTest extends AlinkTestBase {
	StreamOperator<?> inputStreamOp;
	BatchOperator<?> inputBatchOp;
	long[] expectedPrediction;

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
		expectedPrediction = new long[] {0, 0, 0, 1, 1, 1};
	}

	@Test
	public void test() throws Exception {
		BisectingKMeans bisectingKMeans = new BisectingKMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setK(2)
			.setMaxIter(10);
		PipelineModel model = new Pipeline().add(bisectingKMeans).fit(inputBatchOp);
		BatchOperator <?> batchPredOp = model.transform(inputBatchOp)
			.select(new String[] {"id", "pred"});
		verifyPredResult(batchPredOp.collect());
		CollectSinkStreamOp streamPredOp = model.transform(inputStreamOp)
			.select(new String[] {"id", "pred"})
			.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		verifyPredResult(streamPredOp.getAndRemoveValues());
	}

	private void verifyPredResult(List <Row> res) {
		res.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
		long[] pred = res.stream().map(x -> (Long) x.getField(1)).mapToLong(Long::longValue).toArray();
		assertArrayEquals(expectedPrediction, pred);
	}

	@Test
	public void testInitializer() {
		BisectingKMeansModel model = new BisectingKMeansModel();
		assertEquals(model.getParams().size(), 0);
		BisectingKMeans bisectingKMeans = new BisectingKMeans(new Params());
		assertEquals(bisectingKMeans.getParams().size(), 0);

		BisectingKMeansTrainBatchOp op = new BisectingKMeansTrainBatchOp();
		assertEquals(op.getParams().size(), 0);

		BisectingKMeansPredictBatchOp predict = new BisectingKMeansPredictBatchOp(new Params());
		assertEquals(predict.getParams().size(), 0);
		predict = new BisectingKMeansPredictBatchOp();
		assertEquals(predict.getParams().size(), 0);

		BisectingKMeansPredictStreamOp predictStream = new BisectingKMeansPredictStreamOp(op, new Params());
		assertEquals(predictStream.getParams().size(), 0);
		predictStream = new BisectingKMeansPredictStreamOp(predict);
		assertEquals(predictStream.getParams().size(), 0);
	}
}
