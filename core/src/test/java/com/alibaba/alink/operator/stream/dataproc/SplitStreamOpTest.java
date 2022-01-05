package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Tests the {@link SplitStreamOp}.
 */
public class SplitStreamOpTest extends AlinkTestBase {
	Row[] inputRows;
	StreamOperator<?> inputDataOp;

	@Before
	public void before() {
		inputRows = new Row[] {
			Row.of("1L", "1L", 5.0),
			Row.of("2L", "2L", 1.0),
			Row.of("2L", "3L", 2.0),
			Row.of("3L", "1L", 1.0),
			Row.of("3L", "2L", 3.0),
			Row.of("3L", "3L", 0.0),
		};
		inputDataOp = new MemSourceStreamOp(inputRows, new String[] {"uid", "iid", "label"});
	}

	@Test
	public void testSplit() throws Exception {
		SplitStreamOp splitOp = new SplitStreamOp(0.5)
			.linkFrom(inputDataOp);
		CollectSinkStreamOp collectSinkStreamOp1 = splitOp.link(new CollectSinkStreamOp());
		CollectSinkStreamOp collectSinkStreamOp2 = splitOp.getSideOutput(0).link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> output1 = collectSinkStreamOp1.getAndRemoveValues();
		List <Row> output2 = collectSinkStreamOp2.getAndRemoveValues();
		assertEquals(output1.size() + output2.size(), inputRows.length);
	}
}