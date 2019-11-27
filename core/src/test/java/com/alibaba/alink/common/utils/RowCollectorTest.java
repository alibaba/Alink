package com.alibaba.alink.common.utils;

import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for RowCollector.
 */
public class RowCollectorTest {

	@Test
	public void testRowCollector() {
		List<Row> rows = new ArrayList<>(1);

		RowCollector collector = new RowCollector(rows);

		collector.collect(Row.of(2));
		Assert.assertEquals("test size of RowCollector fail", 1, collector.getRows().size());
		Assert.assertEquals("test value of RowCollector fail", 2, collector.getRows().get(0).getField(0));

		collector.clear();
		Assert.assertEquals("test clear of RowCollector fail", 0, collector.getRows().size());

		collector.collect(Row.of(0));

		collector.close();
		Assert.assertEquals("test close of RowCollector fail", 0, collector.getRows().size());
	}
}
