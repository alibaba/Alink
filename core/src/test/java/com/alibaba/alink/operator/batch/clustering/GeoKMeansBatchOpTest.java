package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test
 */

public class GeoKMeansBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, 0),
			Row.of(8, 8),
			Row.of(1, 2),
			Row.of(9, 10),
			Row.of(3, 1),
			Row.of(10, 7)
		};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"f0", "f1"});

		GeoKMeansTrainBatchOp op = new GeoKMeansTrainBatchOp()
			.setK(2)
			.setLatitudeCol("f0")
			.setLongitudeCol("f1")
			.linkFrom(source);

		GeoKMeansPredictBatchOp predict = new GeoKMeansPredictBatchOp()
			.setPredictionCol("pred")
			.linkFrom(op, source);

		Assert.assertEquals(predict.select("pred").distinct().count(), 2);
	}

}