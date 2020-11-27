package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class QuantileDiscretizerTest extends AlinkTestBase {

	@Test
	public void train() {
		try {
			NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 1000, "col0");

			Pipeline pipeline = new Pipeline()
				.add(new QuantileDiscretizer()
					.setNumBuckets(2)
					.setSelectedCols(new String[] {"col0"})
					.enableLazyPrintModelInfo());

			Assert.assertEquals(1001, pipeline.fit(numSeqSourceBatchOp).transform(numSeqSourceBatchOp).collect()
				.size());
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Should not throw exception here.");
		}
	}
}