package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class BaseSqlApiStreamOpTest extends AlinkTestBase {

	Row[] rows = new Row[] {
		Row.of("1L", "1L", 5.0),
		Row.of("2L", "2L", 1.0),
		Row.of("2L", "3L", 2.0),
		Row.of("3L", "1L", 1.0),
		Row.of("3L", "2L", 3.0),
		Row.of("3L", "3L", 0.0),
	};

	@Test
	public void test() {
		StreamOperator data = new MemSourceStreamOp(rows, new String[] {"f1", "f2", "f3"});

		Assert.assertEquals(data.select("f1").getColNames().length, 1);
		Assert.assertEquals(data.select(new String[] {"f1", "f2"}).getColNames().length, 2);
		Assert.assertEquals(new UnionAllStreamOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new WhereStreamOp().setClause("f1='1L'").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new FilterStreamOp().setClause("f1='1L'").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new AsStreamOp().setClause("ff1,ff2,ff3").linkFrom(data).getColNames().length, 3);

		StreamOperator wg = new WindowGroupByStreamOp()
			.setWindowLength(1)
			.setSelectClause("f1,sum(f3)")
			.setGroupByClause("f1");
		wg.linkFrom(data).print();
	}
}