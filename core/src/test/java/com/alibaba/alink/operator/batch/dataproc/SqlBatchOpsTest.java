package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.AsBatchOp;
import com.alibaba.alink.operator.batch.sql.DistinctBatchOp;
import com.alibaba.alink.operator.batch.sql.FilterBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.sql.IntersectBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.batch.sql.MinusBatchOp;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionBatchOp;
import com.alibaba.alink.operator.batch.sql.WhereBatchOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlBatchOpsTest extends AlinkTestBase {

	MemSourceBatchOp createTable1() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"2", -2, 0.9, 2.0, false}),
				Row.of(new Object[] {"3", 100, -0.01, 3.0, true}),
				Row.of(new Object[] {"4", -99, null, 4.0, false}),
				Row.of(new Object[] {"5", 1, 1.1, 5.0, true}),
				Row.of(new Object[] {"6", -2, 0.9, 6.0, false})
			};
		String[] colnames = new String[] {"col1", "col2", "col3", "col4", "col5"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		return inOp;
	}

	MemSourceBatchOp createTable2() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"7", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"8", 1, 1.1, 1.0, true})
			};
		String[] colnames = new String[] {"col1", "col2", "col3", "col4", "col5"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		return inOp;
	}

	MemSourceBatchOp createTable3() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"7", 700}),
				Row.of(new Object[] {"2", 200}),
				Row.of(new Object[] {"1", 100})
			};
		String[] colnames = new String[] {"cola", "colb"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		return inOp;
	}

	MemSourceBatchOp createTable4() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"sadfaeatd weibo_233    wd das dsadr 7", "ad", "ads"}),
				Row.of(new Object[] {"2 daweibo_ewewrwe sf  ad a weibo_3 daf", "ads", "ad"}),
				Row.of(new Object[] {"dareqraewq tweibo_32421 gg weibo_23432424 dsare1", "adfasd", "asd"})
			};
		String[] colnames = new String[] {"cola", "colb", "colc"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		return inOp;
	}

	MemSourceStreamOp createTable5() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"7", 100}),
				Row.of(new Object[] {"2", 100}),
				Row.of(new Object[] {"1", 100}),
				Row.of(new Object[] {"7", 100}),
				Row.of(new Object[] {"2", 100}),
				Row.of(new Object[] {"1", 100})
			};
		String[] colnames = new String[] {"cola", "colb"};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);
		return inOp;
	}

	MemSourceBatchOp createTable6() {
		String v1 = "2.0454685727600008E-4,-0.003931634151376784,-0.0014749639900401236";
		String v2 = "0.0018721634987741709,-0.004380578262498602,5.378552887123078E-4";
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {v1, v2})};
		String[] colnames = new String[] {"cola", "colb"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		return inOp;
	}

	@Test
	public void testSelect() throws Exception {
		MemSourceBatchOp inOp = createTable1();
		SelectBatchOp op = new SelectBatchOp()
			.setClause("col1");
		inOp.link(op).collect();
	}

	@Test
	public void testAs() throws Exception {
		MemSourceBatchOp inOp = createTable1();
		AsBatchOp op = new AsBatchOp()
			.setClause("a");
		inOp.link(op).collect();
	}

	@Test
	public void testDistinct() throws Exception {
		MemSourceBatchOp inOp = createTable2();
		DistinctBatchOp op = new DistinctBatchOp();
		inOp.link(op).collect();
	}

	@Test
	public void testFilter() throws Exception {
		MemSourceBatchOp inOp = createTable1();
		FilterBatchOp op = new FilterBatchOp()
			.setClause("col1='1'");
		inOp.link(op).collect();
	}

	@Test
	public void testRegExp() throws Exception {
		MemSourceBatchOp inOp = createTable4();
		inOp.collect();

		SelectBatchOp op = new SelectBatchOp()
			.setClause("regexp_replace(cola, ' [a-zA-Z0-9]*_[a-zA-Z0-9]* ', '')");
		//        inOp.link(op).lazyPrint(-1);

		op = new SelectBatchOp()
			.setClause("regexp(cola, colb)");
		//        inOp.link(op).lazyPrint(-1);

		inOp.collect();

		op = new SelectBatchOp()
			.setClause("regexp_extract(cola, ' [a-zA-Z0-9]*_[a-zA-Z0-9]* ', 0)");
		inOp.link(op).collect();
	}

	@Test
	public void testNow() throws Exception {
		MemSourceBatchOp inOp = createTable3();
		inOp.lazyPrint(-1);

		SelectBatchOp op = new SelectBatchOp()
			.setClause("NOW(10) as b1, NOW(colb) as b2");
		inOp.link(op).collect();
	}

	@Test
	public void testGroupBy() throws Exception {
		MemSourceBatchOp inOp = createTable2();

		GroupByBatchOp op = new GroupByBatchOp()
			.setGroupByPredicate("col1")
			.setSelectClause("col1,sum(col2),sum(col3)");

		inOp.link(op).collect();
	}

	@Test
	public void testIntersect() throws Exception {
		MemSourceBatchOp inOp1 = createTable1();
		MemSourceBatchOp inOp2 = createTable2();

		IntersectBatchOp op = new IntersectBatchOp();

		op.linkFrom(inOp1, inOp2);
		IntersectBatchOp op2 = new IntersectBatchOp();
		op2.linkFrom(inOp1, inOp2).collect();
	}

	@Test
	public void testJoin() throws Exception {
		MemSourceBatchOp inOp1 = createTable1();
		MemSourceBatchOp inOp2 = createTable3();

		JoinBatchOp inner = new JoinBatchOp()
			.setSelectClause("cola,colb")
			.setJoinPredicate("col1=cola");

		inner.linkFrom(inOp1, inOp2);

		JoinBatchOp left = new JoinBatchOp()
			.setSelectClause("cola,colb")
			.setJoinPredicate("col1=cola")
			.setType("leftOuterJoin");

		left.linkFrom(inOp1, inOp2);

		JoinBatchOp right = new JoinBatchOp()
			.setSelectClause("cola,colb")
			.setJoinPredicate("col1=cola")
			.setType("rightOuterJoin");
		right.linkFrom(inOp1, inOp2);

		JoinBatchOp full = new JoinBatchOp()
			.setSelectClause("b.cola,b.colb")
			.setJoinPredicate("a.col1=b.cola")
			.setType("fullOuterJoin");

		full.linkFrom(inOp1, inOp2).collect();
	}

	@Test
	public void testOrderBy() throws Exception {
		MemSourceBatchOp inOp = createTable3();

		Assert.assertEquals(inOp.orderBy("cola", 2, true).count(), 2);
		Assert.assertEquals(inOp.orderBy("cola", 2, 3).count(), 1);
	}

	@Test
	public void testMinus() throws Exception {
		MemSourceBatchOp inOp1 = createTable1();
		MemSourceBatchOp inOp2 = createTable2();

		MinusBatchOp op = new MinusBatchOp();
		op.linkFrom(inOp1, inOp2).collect();
	}

	@Test
	public void testUnion() throws Exception {
		MemSourceBatchOp inOp1 = createTable1();
		MemSourceBatchOp inOp2 = createTable2();

		UnionBatchOp op = new UnionBatchOp();
		op.linkFrom(inOp1, inOp2).collect();
	}

	@Test
	public void testWhere() throws Exception {
		MemSourceBatchOp inOp = createTable3();

		WhereBatchOp op = new WhereBatchOp()
			.setClause("cola='2'");
		List <BatchOperator <?>> ops = new ArrayList <>();
		ops.add(inOp);
		op.linkFrom(ops).collect();
	}
}
