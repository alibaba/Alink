package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class BaseSqlApiBatchOpTest extends AlinkTestBase {

	Row[] rows = new Row[] {
		Row.of("1L", "1L", 5.0),
		Row.of("2L", "3L", 2.0),
		Row.of("3L", "1L", 1.0),
	};

	Row[] rows1 = new Row[] {
		Row.of("1L", "1L", 15.0),
		Row.of("2L", "3L", 12.0),
		Row.of("4L", "3L", 10.0),
	};
	@Test
	public void test1() throws Exception{
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"f1", "f2", "f3"});
		BatchOperator data1 = new MemSourceBatchOp(rows1, new String[] {"f1", "f2", "f3"});

		new FullOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1")
			.setSelectClause("case when a.f1 is null then b.f1 when b.f1 is null then a.f1 else b.f1 end as uid, "
						   + "case when a.f1 is null then b.f3 when b.f1 is null then a.f3 else b.f3 end as factors")
			.linkFrom(data, data1).print();
	}
	@Test
	public void test() {
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"f1", "f2", "f3"});
		Assert.assertEquals(data.select("f1").getColNames().length, 1);
		Assert.assertEquals(data.select(new String[] {"f1", "f2"}).getColNames().length, 2);
		Assert.assertEquals(new JoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new LeftOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new RightOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new FullOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new MinusBatchOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new MinusAllBatchOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new UnionBatchOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new UnionAllBatchOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new IntersectBatchOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new IntersectAllBatchOp().linkFrom(data, data).getColNames().length, 3);
		Assert.assertEquals(new DistinctBatchOp().linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new WhereBatchOp().setClause("f1='1L'").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new FilterBatchOp().setClause("f1='1L'").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new GroupByBatchOp().setGroupByPredicate("f1").setSelectClause("f1, sum(f3)")
			.linkFrom(data).getColNames().length, 2);
		Assert.assertEquals(new AsBatchOp().setClause("ff1,ff2,ff3").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new OrderByBatchOp().setClause("f1").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 2).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 2, true).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 0, 1).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 0, 1, false).getColNames().length, 3);
	}
}