package com.alibaba.alink.common.io.directreader;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test cases for DirectReader
 */
public class DirectReaderTest {
	private String[] inputArray;
	private BatchOperator input;

	@Before
	public void setUp() throws Exception {
		inputArray = new String[]{"a", "b", "c"};
		input = new MemSourceBatchOp(inputArray, "col0");
	}

	@Test
	public void testDirectRead() {
		Set<String> inputSet = new HashSet<>(Arrays.asList(inputArray));

		List<Row> collected = DirectReader.directRead(input);

		Assert.assertEquals(inputSet.size(), collected.size());
		for (Row r : collected) {
			Assert.assertTrue(inputSet.contains(r.getField(0)));
		}
	}
}