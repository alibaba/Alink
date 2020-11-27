package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class EvalClusterBatchOpTest extends AlinkTestBase {
	private static Row[] rows = new Row[] {
		Row.of("a", "0,0,0"),
		Row.of("a", "0.1,0.1,0.1"),
		Row.of("a", "0.2,0.2,0.2"),
		Row.of("b", "9,9,9"),
		Row.of("b", "9.1,9.1,9.1"),
		Row.of("b", "9.2,9.2,9.2")
	};

	@Test
	public void test() throws Exception {
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"label", "Y"});

		KMeans train = new KMeans()
			.setVectorCol("Y")
			.setPredictionCol("pred")
			.setK(2);

		ClusterMetrics metrics = new EvalClusterBatchOp(new Params())
			.setPredictionCol("pred")
			.setVectorCol("Y")
			.setLabelCol("label")
			.linkFrom(train.fit(inOp).transform(inOp))
			.collectMetrics();

		Assert.assertEquals(metrics.getVrc(), 12150.00, 0.01);
		Assert.assertEquals(metrics.getCp(), 0.115, 0.01);
		Assert.assertEquals(metrics.getCount().intValue(), 6);
		Assert.assertEquals(metrics.getDb(), 0.014, 0.01);
		Assert.assertEquals(metrics.getSp(), 15.58, 0.01);
		Assert.assertEquals(metrics.getK().intValue(), 2);
		Assert.assertEquals(metrics.getSsb(), 364.5, 0.01);
		Assert.assertEquals(metrics.getSsw(), 0.119, 0.01);
		Assert.assertEquals(metrics.getPurity(), 1.0, 0.01);
		Assert.assertEquals(metrics.getNmi(), 1.0, 0.01);
		Assert.assertEquals(metrics.getAri(), 1.0, 0.01);
		Assert.assertEquals(metrics.getRi(), 1.0, 0.01);
		Assert.assertEquals(metrics.getSilhouetteCoefficient(), 0.99, 0.01);
		System.out.println(metrics.toString());
	}

	@Test
	public void testNoVector() throws Exception {
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"label", "Y"});

		KMeans train = new KMeans()
			.setVectorCol("Y")
			.setPredictionCol("pred")
			.setK(2);

		ClusterMetrics metrics = new EvalClusterBatchOp()
			.setPredictionCol("pred")
			.linkFrom(train.fit(inOp).transform(inOp))
			.collectMetrics();

		Assert.assertEquals(metrics.getCount().intValue(), 6);
		Assert.assertArrayEquals(metrics.getClusterArray(), new String[] {"0", "1"});
	}
}