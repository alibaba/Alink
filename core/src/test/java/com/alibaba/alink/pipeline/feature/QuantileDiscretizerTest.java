package com.alibaba.alink.pipeline.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QuantileDiscretizerTest {

	@Test
	public void train() {
		try {
			NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 1000, "col0");

			Pipeline pipeline = new Pipeline()
				.add(new QuantileDiscretizer()
					.setNumBuckets(2)
					.setSelectedCols(new String[] {"col0"}));

			List<Row> result = pipeline.fit(numSeqSourceBatchOp).transform(numSeqSourceBatchOp).collect();
		} catch (Exception ex) {
			Assert.fail("Should not throw exception here.");
		}
	}
}