package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test for WeightSampleBatchOp.
 */

public class WeightSampleBatchOpTest extends AlinkTestBase {

	private String[] colnames = new String[] {"id", "weight", "col0"};

	private MemSourceBatchOp getSourceBatchOp() {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1.3, 1.1),
				Row.of("b", 2.5, 0.9),
				Row.of("c", 100.2, -0.01),
				Row.of("d", 99.9, 100.9),
				Row.of("e", 1.4, 1.1),
				Row.of("f", 2.2, 0.9),
				Row.of("g", 100.9, -0.01),
				Row.of("j", 99.5, 100.9)
			};
		return new MemSourceBatchOp(Arrays.asList(testArray), colnames);
	}

	@Test
	public void testWithoutReplacement() throws Exception {
		WeightSampleBatchOp sampleBatchOp = new WeightSampleBatchOp()
			.setWeightCol("weight")
			.setRatio(0.5)
			.linkFrom(getSourceBatchOp());
		Assert.assertEquals(sampleBatchOp.count(), 4);
	}

	@Test
	public void testWithReplacement() throws Exception {
		WeightSampleBatchOp sampleBatchOp = new WeightSampleBatchOp()
			.setWeightCol("weight")
			.setRatio(0.5)
			.setWithReplacement(true)
			.linkFrom(getSourceBatchOp());
		Assert.assertEquals(sampleBatchOp.count(), 4);
	}
}