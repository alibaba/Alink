package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class SplitStreamOpTest extends AlinkTestBase {
	Row[] rows = new Row[] {
		Row.of("1L", "1L", 5.0),
		Row.of("2L", "2L", 1.0),
		Row.of("2L", "3L", 2.0),
		Row.of("3L", "1L", 1.0),
		Row.of("3L", "2L", 3.0),
		Row.of("3L", "3L", 0.0),
	};

	@Test
	public void linkFrom() throws Exception {
		StreamOperator data = new MemSourceStreamOp(rows, new String[] {"uid", "iid", "label"});
		new SplitStreamOp().setFraction(0.5).linkFrom(data).print();
		StreamOperator.execute();
	}

}