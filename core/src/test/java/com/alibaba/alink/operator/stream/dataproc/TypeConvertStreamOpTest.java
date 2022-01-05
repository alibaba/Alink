package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TypeConvertStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("1", 1, 1.1, 1.0, true),
				Row.of("2", -2, 0.9, 2.0, false),
				Row.of("3", 100, -0.01, 3.0, true),
				Row.of("4", -99, null, 4.0, false),
				Row.of("5", 1, 1.1, 5.0, true),
				Row.of("6", -2, 0.9, 6.0, false)
			};

		String[] colNames = new String[] {"group", "col2", "col3", "col4", "col5"};

		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colNames);

		TypeConvertStreamOp typeConvertStreamOp = new TypeConvertStreamOp()
			.setSelectedCols(new String[] {colNames[0], colNames[1], colNames[2], colNames[3], colNames[4]})
			.setTargetType("bigint");

		TypeConvertStreamOp typeConvertStreamOp1 = new TypeConvertStreamOp()
			.setTargetType("string");

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp();
		CollectSinkStreamOp collectSinkStreamOp1 = new CollectSinkStreamOp();

		inOp.link(typeConvertStreamOp).link(collectSinkStreamOp);
		inOp.link(typeConvertStreamOp1).link(collectSinkStreamOp1);

		StreamOperator.execute();

		MTable ret = new MTable(collectSinkStreamOp.getAndRemoveValues(), typeConvertStreamOp.getSchema());
		MTable ret1 = new MTable(collectSinkStreamOp1.getAndRemoveValues(), typeConvertStreamOp1.getSchema());

		Assert.assertEquals(6, ret.getNumRow());
		Assert.assertEquals(6, ret1.getNumRow());

		ret.orderBy(0);
		ret1.orderBy(0);

		Assert.assertEquals(Row.of(1L, 1L, 1L, 1L, 1L), ret.getTable().get(0));
		Assert.assertEquals(Row.of("1", "1", "1.1", "1.0", "true"), ret1.getTable().get(0));
	}
}
