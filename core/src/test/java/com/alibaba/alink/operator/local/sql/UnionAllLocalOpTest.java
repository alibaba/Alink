package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UnionAllLocalOpTest {
	@Test
	public void testUnionAllLocalOp() {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		LocalOperator <?> data1 = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));
		LocalOperator <?> data2 = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));
		new UnionAllLocalOp()
			.linkFrom(data1, data2)
			.print();
	}
}