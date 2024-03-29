package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class EvalMultiClassLocalOpTest extends TestCase {

	@Test
	public void test() throws Exception {

		Row[] data =
			new Row[] {
				Row.of("prefix0", "{\"prefix0\": 0.3, \"prefix1\": 0.2, \"prefix2\": 0.5}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.3, \"prefix1\": 0.4, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.5, \"prefix1\": 0.2, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix2", "{\"prefix0\": 0.6, \"prefix1\": 0.1, \"prefix2\": 0.3}"),
				Row.of("prefix1", null),
				Row.of("prefix1", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}")
			};

		MultiClassMetrics metrics = new MemSourceLocalOp(data, new String[] {"label", "detailInput"})
			.link(
				new EvalMultiClassLocalOp()
					.setLabelCol("label")
					.setPredictionDetailCol("detailInput")
			)
			.collectMetrics();

		Assert.assertEquals(0.538, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.301, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.538, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.538, metrics.getWeightedRecall(), 0.01);
		Assert.assertEquals(0.444, metrics.getF1("prefix0"), 0.01);
		Assert.assertEquals(1.042, metrics.getLogLoss(), 0.01);
		Assert.assertEquals(0.538, metrics.getMicroSensitivity(), 0.01);
		Assert.assertEquals(0.777, metrics.getMacroSpecificity(), 0.01);

		System.out.println(metrics.toString());
	}
}