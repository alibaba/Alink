package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GroupByBatchOpTest extends AlinkTestBase {

	@Test
	public void testGroupByBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		BatchOperator <?> batch_data = new MemSourceBatchOp(df, "f1 string, f2 int, f3 double");
		BatchOperator <?> op = new GroupByBatchOp().setGroupByPredicate("f1").setSelectClause("f1,avg(f2) as f2");
		batch_data = batch_data.link(op);
		batch_data.print();
	}

	@Test
	public void testPattern() {
		TableSchema tableSchema = new TableSchema(new String[] {"val1", "g1", "val2", "g2"},
			new TypeInformation[] {Types.STRING, Types.SQL_DATE, Types.DOUBLE, AlinkTypes.SPARSE_VECTOR});

		String[] groupCols = new String[] {"g1","g2"};
		String str = "mtable_agg(val1), cos(val), Mtable_AGG(val1,val2), mtable_Agg()";
		String[] mTableAggNames = new String[] {"aa1", "aa2", "aa3"};
		String modStr = new GroupByBatchOp().modifyMTableClasuse(groupCols, str, tableSchema, mTableAggNames);

		Assert.assertEquals("aa1(`val1`), cos(val), aa2(`val1`,`val2`), aa3(`val1`,`val2`)",
			modStr);
	}

	@Test
	public void testIsPattern() {
		Assert.assertTrue(GroupByBatchOp.isHasMTableClause("mtable_agg(val1), cos(val)"));
		Assert.assertTrue(GroupByBatchOp.isHasMTableClause("mTable_agg(val1), cos(val))"));
		Assert.assertTrue(GroupByBatchOp.isHasMTableClause("mtable_Agg(val1), cos(val)"));
		Assert.assertTrue(GroupByBatchOp.isHasMTableClause("MTable_AGG(val1), cos(val)"));
		Assert.assertFalse(GroupByBatchOp.isHasMTableClause("MTableAGG(val1), cos(val)"));
		Assert.assertFalse(GroupByBatchOp.isHasMTableClause("cos(val)"));
		Assert.assertFalse(GroupByBatchOp.isHasMTableClause("MTable_AGG, cos(val)"));
	}

	@Test
	public void testWithMTable() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		BatchOperator <?> batch_data = new MemSourceBatchOp(df, "f1 string, f2 int, f3 double");
		BatchOperator <?> op = new GroupByBatchOp().setGroupByPredicate("f1").setSelectClause(
			"f1,mtable_agg(f2, f3) as f2, mtable_agg() as f3");

		batch_data = batch_data.link(op);
		List <Row> rows = batch_data.collect();
		rows.sort(new RowComparator(0));

		Assert.assertEquals(2, rows.size());
		Assert.assertEquals("Nevada", rows.get(0).getField(0));
		Assert.assertEquals("{\"data\":{\"f2\":[2001,2002,2003],\"f3\":[2.4,2.9,3.2]},\"schema\":\"f2 INT,f3 "
				+ "DOUBLE\"}",
			JsonConverter.toJson(rows.get(0).getField(1)));
		Assert.assertEquals("Ohio", rows.get(1).getField(0));
		Assert.assertEquals("{\"data\":{\"f2\":[2000,2001,2002],\"f3\":[1.5,1.7,3.6]},\"schema\":\"f2 INT,f3 "
				+ "DOUBLE\"}",
			JsonConverter.toJson(rows.get(1).getField(1)));
	}
}
