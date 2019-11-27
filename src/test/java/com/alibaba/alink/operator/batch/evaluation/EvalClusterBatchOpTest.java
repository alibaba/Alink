package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;

import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.pipeline.clustering.KMeans;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class EvalClusterBatchOpTest {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "0,0,0"),
			Row.of(0, "0.1,0.1,0.1"),
			Row.of(0, "0.2,0.2,0.2"),
			Row.of(1, "9,9,9"),
			Row.of(1, "9.1,9.1,9.1"),
			Row.of(1, "9.2,9.2,9.2")
		};

		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"label", "Y"});

		KMeans train = new KMeans()
			.setVectorCol("Y")
			.setPredictionCol("pred")
			.setK(2);

		ClusterMetrics metrics = new EvalClusterBatchOp()
			.setPredictionCol("pred")
			.setVectorCol("Y")
			.setLabelCol("label")
			.linkFrom(train.fit(inOp).transform(inOp))
			.collectMetrics();

		Assert.assertEquals(metrics.getCalinskiHarabaz(), 12150.00, 0.01);
		Assert.assertEquals(metrics.getCompactness(), 0.115, 0.01);
		Assert.assertEquals(metrics.getCount().intValue(), 6);
		Assert.assertEquals(metrics.getDaviesBouldin(), 0.014, 0.01);
		Assert.assertEquals(metrics.getSeperation(), 15.58, 0.01);
		Assert.assertEquals(metrics.getK().intValue(), 2);
		Assert.assertEquals(metrics.getSsb(), 364.5, 0.01);
		Assert.assertEquals(metrics.getSsw(), 0.119, 0.01);
		Assert.assertEquals(metrics.getPurity(), 1.0, 0.01);
		Assert.assertEquals(metrics.getNmi(), 1.0, 0.01);
		Assert.assertEquals(metrics.getAri(), 1.0, 0.01);
		Assert.assertEquals(metrics.getRi(), 1.0, 0.01);
		Assert.assertEquals(metrics.getSilhouetteCoefficient(), 0.99,0.01);
	}
}