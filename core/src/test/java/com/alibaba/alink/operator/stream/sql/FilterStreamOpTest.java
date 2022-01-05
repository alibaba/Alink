package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FilterStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
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
		String[] colNames = new String[] {"col1", "col2", "col3"};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colNames);

		String filter = "col1='a'";
		FilterStreamOp filterStreamOp = new FilterStreamOp(filter);

		CollectSinkStreamOp op = inOp
			.link(filterStreamOp)
			.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		Assert.assertEquals(2, op.getAndRemoveValues().size());
	}
}
