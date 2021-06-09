package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

public class MemoryDataBridgeGeneratorTest extends AlinkTestBase {
	private String[] inputArray;
	private BatchOperator input;

	@Before
	public void setUp() throws Exception {
		inputArray = new String[] {"a", "b", "c"};
		input = new MemSourceBatchOp(inputArray, "col0");
	}

	@Test
	public void generate() {
		Assert.assertArrayEquals(
			inputArray,
			new MemoryDataBridgeGenerator()
				.generate(input, null)
				.read(new FilterFunction <Row>() {
					private static final long serialVersionUID = 7255987093563375997L;

					@Override
					public boolean filter(Row value) throws Exception {
						return true;
					}
				})
				.stream()
				.map(new Function <Row, String>() {
					@Override
					public String apply(Row row) {
						return (String) row.getField(0);
					}
				})
				.toArray()
		);
	}
}