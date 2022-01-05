package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
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

/**
 * Tests the {@link GeoKMeans}.
 */
public class GeoKMeansTest extends AlinkTestBase {
	StreamOperator<?> inputStreamOp;
	BatchOperator<?> inputBatchOp;
	double[] expectedPrediction;

	@Before
	public void before() {
		Row[] rows = new Row[] {
			Row.of(0, 0, 0),
			Row.of(1, 8, 8),
			Row.of(2, 1, 2),
			Row.of(3, 9, 10),
			Row.of(4, 3, 1),
			Row.of(5, 10, 7)
		};
		inputBatchOp = new TableSourceBatchOp(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "f0", "f1"})
		);
		inputStreamOp = new TableSourceStreamOp(
			MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "f0", "f1"})
		);
		expectedPrediction = new double[] {185.31, 117.08, 117.18, 183.04, 185.32, 183.70};
	}

	@Test
	public void testGeoKmeans() throws Exception {
		GeoKMeans geoKMeans = new GeoKMeans()
			.setLatitudeCol("f0")
			.setLongitudeCol("f1")
			.setPredictionCol("pred")
			.setPredictionDistanceCol("distance")
			.setK(2);
		PipelineModel model = new Pipeline().add(geoKMeans).fit(inputBatchOp);
		BatchOperator<?> batchPredOp = model.transform(inputBatchOp);
		verifyPredResult(batchPredOp.collect());

		CollectSinkStreamOp streamPredOp = model.transform(inputStreamOp).link(new CollectSinkStreamOp());
		StreamOperator.execute();
		verifyPredResult(streamPredOp.getAndRemoveValues());
	}

	private void verifyPredResult(List <Row> res) {
		res.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
		double[] pred = res.stream().map(x -> (Double) x.getField(4)).mapToDouble(Double::doubleValue).toArray();
		assertArrayEquals(expectedPrediction, pred, 0.01);
	}
}