package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDTFBatchOpTest extends AlinkTestBase {

	@Test
	public void testUdtf() throws Exception {
		MemSourceBatchOp src = new MemSourceBatchOp(new Object[][] {
			new Object[] {"1", "a b", 1L},
			new Object[] {"2", "b33 bb44", 2L},
			new Object[] {"3", "", 3L}
		}, new String[] {"c0", "c1", "c2"});

		UDTFBatchOp udtfOp = new UDTFBatchOp()
			.setFunc(new Split(" "))
			.setSelectedCols("c1", "c2")
			.setReservedCols(new String[] {"c1", "c2"})
			.setOutputCols("c1", "length");
		udtfOp.linkFrom(src);

		// the column c1 in the input table is shadowed
		Assert.assertArrayEquals(udtfOp.getColNames(), new String[] {"c2", "c1", "length"});

		List <Row> expected = new ArrayList <>(Arrays.asList(
			Row.of(1L, "a", 2L),
			Row.of(1L, "b", 2L),
			Row.of(2L, "b33", 5L),
			Row.of(2L, "bb44", 6L)
		));
		List <Row> result = udtfOp.collect();
		Assert.assertEquals(result, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyFunc() throws Exception {
		MemSourceBatchOp src = new MemSourceBatchOp(new Object[][] {
			new Object[] {"1", "a b", 1L},
			new Object[] {"2", "b33 bb44", 2L},
			new Object[] {"3", "", 3L}
		}, new String[] {"c0", "c1", "c2"});

		UDTFBatchOp udtfOp = new UDTFBatchOp()
			.setSelectedCols("c1", "c2")
			.setReservedCols(new String[] {"c1", "c2"})
			.setOutputCols("c1", "length");
		udtfOp.linkFrom(src);
	}
}
