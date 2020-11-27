package com.alibaba.alink.operator.batch.source;

import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class NumSeqSourceBatchOpTest extends AlinkTestBase {

	@Test
	public void testConstructorFromTo() {
		NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 1);
		Assert.assertArrayEquals(numSeqSourceBatchOp.collect().toArray(new Row[0]), new Row[] {Row.of(0L), Row.of
			(1L)});
	}

	@Test
	public void testConstructorParams() {
		NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp()
			.setFrom(0)
			.setTo(1);

		Assert.assertArrayEquals(numSeqSourceBatchOp.collect().toArray(new Row[0]), new Row[] {Row.of(0L), Row.of
			(1L)});
	}
}