package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for BinaryClassEvaluation.
 */
public class EvalBinaryClassBatchOpTest {
	@Test
	public void test() throws Exception {
		Row[] data =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"),
				Row.of("prefix1", "{\"prefix1\": 0.65, \"prefix0\": 0.35}"),
				Row.of("prefix0", null),
				Row.of("prefix1", "{\"prefix1\": 0.55, \"prefix0\": 0.45}"),
				Row.of("prefix0", "{\"prefix1\": 0.4, \"prefix0\": 0.6}"),
				Row.of("prefix0", "{\"prefix1\": 0.3, \"prefix0\": 0.7}"),
				Row.of("prefix1", "{\"prefix1\": 0.35, \"prefix0\": 0.65}"),
				Row.of("prefix0", "{\"prefix1\": 0.2, \"prefix0\": 0.8}"),
				Row.of("prefix1", "{\"prefix1\": 0.1, \"prefix0\": 0.9}")
			};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		BinaryClassMetrics metrics = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.linkFrom(source)
			.collectMetrics();

		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
	}
}