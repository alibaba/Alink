package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class TypeConvertStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"2", -2, 0.9, 2.0, false}),
				Row.of(new Object[] {"3", 100, -0.01, 3.0, true}),
				Row.of(new Object[] {"4", -99, null, 4.0, false}),
				Row.of(new Object[] {"5", 1, 1.1, 5.0, true}),
				Row.of(new Object[] {"6", -2, 0.9, 6.0, false})
			};
		String[] colnames = new String[] {"group", "col2", "col3", "col4", "col5"};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		TypeConvertStreamOp typeConvertStreamOp = new TypeConvertStreamOp().setSelectedCols(
			new String[] {colnames[0], colnames[1], colnames[2], colnames[3], colnames[4]}).setTargetType("bigint");

		TypeConvertStreamOp typeConvertStreamOp1 = new TypeConvertStreamOp()
			.setTargetType("string");

		inOp.link(typeConvertStreamOp).print();
		inOp.link(typeConvertStreamOp1).print();
		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}
}
