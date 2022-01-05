package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests the {@link StratifiedSampleStreamOp}.
 */
public class StratifiedSampleStreamOpTest extends AlinkTestBase {
	Row[] inputRows;
	String[] colNames;
	StreamOperator<?> inputDataOp;

	@Before
	public void before() {
		inputRows =
			new Row[] {
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9)
			};
		colNames = new String[] {"col1", "col2", "col3"};
		inputDataOp = new MemSourceStreamOp(Arrays.asList(inputRows), colNames);
	}

	@Test
	public void test() throws Exception {
		StratifiedSampleStreamOp stratifiedSampleStreamOp = new StratifiedSampleStreamOp()
			.setStrataCol(colNames[0])
			.setStrataRatio(0.5)
			.linkFrom(inputDataOp);
		CollectSinkStreamOp outputStreamOp = stratifiedSampleStreamOp.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		assertTrue(outputStreamOp.getAndRemoveValues().size() <= inputRows.length);
	}
}
