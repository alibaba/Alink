package com.alibaba.alink.operator.stream.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.StratifiedSampleStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class StratifiedSampleStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1, 1.1}),
				Row.of(new Object[] {"b", -2, 0.9}),
				Row.of(new Object[] {"c", 100, -0.01}),
				Row.of(new Object[] {"d", -99, 100.9}),
				Row.of(new Object[] {"a", 1, 1.1}),
				Row.of(new Object[] {"b", -2, 0.9}),
				Row.of(new Object[] {"c", 100, -0.01}),
				Row.of(new Object[] {"d", -99, 100.9})
			};
		String[] colnames = new String[] {"col1", "col2", "col3"};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		StratifiedSampleStreamOp stratifiedSampleStreamOp = new StratifiedSampleStreamOp()
			.setStrataCol(colnames[0])
			.setStrataRatio(0.5);

		StreamOperator op = inOp.link(stratifiedSampleStreamOp);
		op.print();

		StreamOperator.execute();
	}
}
