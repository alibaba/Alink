package com.alibaba.alink.operator.local.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;

public class OneHotTrainLocalOpTest extends TestCase {

	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1, 1),
				Row.of("b", -2, 0.9, 1),
				Row.of("c", 100, -0.01, 1),
				Row.of("e", -99, 100.9, 2),
				Row.of("a", 1, 1.1, 2),
				Row.of("b", -2, 0.9, 1),
				Row.of("c", 100, -0.01, 2),
				Row.of("d", -99, 100.9, 2),
				Row.of(null, null, 1.1, 1)
			};

		String[] colnames = new String[] {"col1", "col2", "col3", "label"};
		MemSourceLocalOp sourceLocalOp = new MemSourceLocalOp(Arrays.asList(testArray), colnames);

		OneHotTrainLocalOp op = new OneHotTrainLocalOp()
			.setSelectedCols("col1")
			.linkFrom(sourceLocalOp);

		OneHotPredictLocalOp predictLocalOp = new OneHotPredictLocalOp()
			.setOutputCols("output")
			.linkFrom(op, sourceLocalOp);

		predictLocalOp.print();
	}

}