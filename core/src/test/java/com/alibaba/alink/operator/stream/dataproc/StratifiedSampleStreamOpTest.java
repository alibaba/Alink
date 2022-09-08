package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;

import static org.junit.Assert.assertTrue;

import junit.framework.Assert;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests the {@link StratifiedSampleStreamOp}.
 */
public class StratifiedSampleStreamOpTest extends AlinkTestBase {
	private String[] colnames = new String[] {"col1", "col2", "col3"};

	private MemSourceStreamOp getSourceStreamOp() {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1.3, 1.1),
				Row.of("b", -2.5, 0.9),
				Row.of("c", 100.2, -0.01),
				Row.of("d", -99.9, 100.9),
				Row.of("a", 1.4, 1.1),
				Row.of("b", -2.2, 0.9),
				Row.of("c", 100.9, -0.01),
				Row.of("d", -99.5, 100.9)
			};
		return new MemSourceStreamOp(Arrays.asList(testArray), colnames);
	}

	@Test
	public void testStringStrataRatios() throws Exception {
		CollectSinkStreamOp output = new StratifiedSampleStreamOp()
			.setStrataCol(colnames[0])
			.setStrataRatios("a:0.5,b:0.5,c:0.5,d:1.0")
			.linkFrom(getSourceStreamOp())
			.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		assertTrue(output.getAndRemoveValues().size() <= 8);
	}

	@Test
	public void testNonStringStrataRatios() throws Exception {
		CollectSinkStreamOp output = new StratifiedSampleStreamOp()
			.setStrataCol(colnames[2])
			.setStrataRatios("1.1:0.5,0.9:0.5,-0.01:0.5,100.9:1.0")
			.linkFrom(getSourceStreamOp())
			.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		assertTrue(output.getAndRemoveValues().size() <= 8);
	}

	@Test
	public void testInCompleteRatios() throws Exception {
		CollectSinkStreamOp output = new StratifiedSampleStreamOp()
			.setStrataCol(colnames[0])
			.setStrataRatios("a:0.5,b:0.5,c:0.5")
			.linkFrom(getSourceStreamOp())
			.link(new CollectSinkStreamOp());
		try {
			StreamOperator.execute();
			assert false;
		} catch (Exception e) {
			Assert.assertEquals("Illegal ratio  for [d]. "
				+ "Please set proper values for ratio or ratios.", ExceptionUtils.getRootCause(e).getMessage());
		}
	}

	@Test
	public void testInCompleteRatiosWithRatioSet() throws Exception {
		CollectSinkStreamOp output = new StratifiedSampleStreamOp()
			.setStrataCol(colnames[0])
			.setStrataRatio(0.5)
			.setStrataRatios("a:0.5,b:0.5,c:0.5")
			.linkFrom(getSourceStreamOp())
			.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		assertTrue(output.getAndRemoveValues().size() <= 8);
	}
}