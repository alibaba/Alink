package com.alibaba.alink.operator.stream.utils;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Assert;
import org.junit.Test;

public class UDTFStreamOpTest {

	@Test
	public void testUdtf() throws Exception {
		MemSourceStreamOp src = new MemSourceStreamOp(new Object[][] {
			new Object[] {"1", "a b", 1L},
			new Object[] {"2", "b33 bb44", 2L},
			new Object[] {"3", "", 3L}
		}, new String[] {"c0", "c1", "c2"});

		UDTFStreamOp udtfOp = new UDTFStreamOp()
			.setFunc(new Split(" "))
			.setSelectedCols("c1", "c2")
			.setReservedCols(new String[] {"c1", "c2"})
			.setOutputCols("c1", "length");
		udtfOp.linkFrom(src);

		Assert.assertArrayEquals(new String[] {"c2", "c1", "length"}, udtfOp.getColNames());
		udtfOp.print();

		StreamOperator.execute();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUdtfEmptyFunc() throws Exception {
		MemSourceStreamOp src = new MemSourceStreamOp(new Object[][] {
			new Object[] {"1", "a b", 1L},
			new Object[] {"2", "b33 bb44", 2L},
			new Object[] {"3", "", 3L}
		}, new String[] {"c0", "c1", "c2"});

		UDTFStreamOp udtfOp = new UDTFStreamOp()
			.setSelectedCols("c1", "c2")
			.setReservedCols(new String[] {"c1", "c2"})
			.setOutputCols("c1", "length");
		udtfOp.linkFrom(src);
	}
}
