package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class OverWindowBatchOpTest extends AlinkTestBase {
	@Test
	public void testMTable() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(4, "Ohio", 2000, 1.5),
			Row.of(5, "Ohio", 2001, 1.7),
			Row.of(6, "Ohio", 2002, 3.6),
			Row.of(1, "Nevada", 2001, 2.4),
			Row.of(2, "Nevada", 2002, 2.9),
			Row.of(3, "Nevada", 2003, 3.2)
		);
		BatchOperator <?> batch_data = new MemSourceBatchOp(df, "id int, f1 string, f2 int, f3 double");
		BatchOperator <?> op = new OverWindowBatchOp()
			.setClause("MTABLE_AGG(f2,f3) as mtable_data, sum(f3) as sum0")
			.setOrderBy("f2")
			.setReservedCols("id", "f1")
			.setGroupCols("f1");
		batch_data = batch_data.link(op);

		//for test two execution; two execution must have the same result.
		List <Row> rowsTmp = batch_data.collect();

		List <Row> resultRows = batch_data.collect();
		resultRows.sort(new RowComparator(0));

		Assert.assertEquals(6, resultRows.size());
		assertR(resultRows.get(0),
			1,
			"Nevada",
			"{\"data\":{\"f2\":[2001],\"f3\":[2.4]},\"schema\":\"f2 INT,f3 DOUBLE\"}",
			2.4);

		assertR(resultRows.get(1),
			2,
			"Nevada",
			"{\"data\":{\"f2\":[2001,2002],\"f3\":[2.4,2.9]},\"schema\":\"f2 INT,f3 DOUBLE\"}",
			5.3);

		assertR(resultRows.get(2),
			3,
			"Nevada",
			"{\"data\":{\"f2\":[2001,2002,2003],\"f3\":[2.4,2.9,3.2]},\"schema\":\"f2 INT,f3 DOUBLE\"}",
			8.5);

		assertR(resultRows.get(3),
			4,
			"Ohio",
			"{\"data\":{\"f2\":[2000],\"f3\":[1.5]},\"schema\":\"f2 INT,f3 DOUBLE\"}",
			1.5);

		assertR(resultRows.get(4),
			5,
			"Ohio",
			"{\"data\":{\"f2\":[2000,2001],\"f3\":[1.5,1.7]},\"schema\":\"f2 INT,f3 DOUBLE\"}",
			3.2);

		assertR(resultRows.get(5),
			6,
			"Ohio",
			"{\"data\":{\"f2\":[2000,2001,2002],\"f3\":[1.5,1.7,3.6]},\"schema\":\"f2 INT,f3 DOUBLE\"}",
			6.8);
	}

	private void assertR(Row row, int expectedId, String expectedF1, String expectedMtableStr, double expectedSum) {
		Assert.assertEquals(expectedId, row.getField(0));
		Assert.assertEquals(expectedF1, row.getField(1));
		Assert.assertEquals(expectedMtableStr, JsonConverter.toJson(row.getField(2)));
		Assert.assertEquals(expectedSum, (double) row.getField(3), 10e-6);
	}

	@Test
	public void testOverWindowBatchOp() throws Exception {
		Row[] rows = new Row[] {
			Row.of(2, 8, 2.2),
			Row.of(2, 7, 3.3),
			Row.of(5, 6, 4.4),
			Row.of(5, 6, 5.5),
			Row.of(7, 5, 6.6),
			Row.of(1, 8, 1.1),
			Row.of(1, 9, 1.0),
			Row.of(7, 5, 7.7),
			Row.of(9, 5, 8.8),
			Row.of(9, 4, 9.8),
			Row.of(19, 4, 8.8),
		};

		MemSourceBatchOp source = new MemSourceBatchOp(rows, new String[] {"id1", "id2", "ip"});

		String clause =
			"STDDEV_SAMP(ip) as stdsam, "
			+ "STDDEV_POP(ip) as stdpop, "
			+ "VAR_SAMP(ip) as varsamp, "
			+ "VAR_POP(ip) as varpop, "
			+ "count(ip) as countip, "
			+ "avg(ip) as avgip, "
			+ "min(ip) as minip, "
			+ "max(ip) as maxip, "
			+ "lag(ip) as lagip, "
			+ "last_distinct(ip, ip) as last_distinctip,"
			+ "last_value(ip) as last_valueip,"
			+ "sum_last(ip) as sum_lastip, "
			+ "listagg(ip) as listaggip, "
			+ "mode(ip) as modeip, "
			+ "square_sum(ip) as square_sumip, "
			+ "median(ip) as medianip,"
			+ "freq(ip) as freqip, "
			+ "is_exist(ip) as is_existip";

		System.out.println(clause);

		OverWindowBatchOp batchWindow = new OverWindowBatchOp()
			.setOrderBy("id1 asc")
			.setClause(clause);
		batchWindow.linkFrom(source).print();

	}

	@Test
	public void test2Cols() throws Exception {
		Row[] rows = new Row[] {
			Row.of(2, 8, 2.2),
			Row.of(2, 7, 3.3),
			Row.of(5, 6, 4.4),
			Row.of(5, 6, 5.5),
			Row.of(7, 5, 6.6),
			Row.of(1, 8, 1.1),
			Row.of(1, 9, 1.0),
			Row.of(7, 5, 7.7),
			Row.of(9, 5, 8.8),
			Row.of(9, 4, 9.8),
		};

		MemSourceBatchOp source = new MemSourceBatchOp(rows, new String[] {"id1", "id2", "value2"});

		String clause = "rank() as rank_data, dense_rank() as dense_rank_data,"
			+ " row_number() as row_number_data,"
			+ " count_preceding(*) as cc";
		OverWindowBatchOp batchWindow = new OverWindowBatchOp()
			.setOrderBy("id1, id2 desc")
			.setClause(clause);
		batchWindow.linkFrom(source).print();

	}

	@Test
	public void test2Partition() throws Exception {
		Row[] rows = new Row[] {
			Row.of(2, 8, 1.0),
			Row.of(2, 7, 1.0),
			Row.of(5, 6, 1.0),
			Row.of(5, 6, 1.0),
			Row.of(7, 5, 1.0),
			Row.of(1, 9, 1.0),
			Row.of(1, 9, 1.0),
			Row.of(7, 5, 1.0),
			Row.of(9, 5, 1.0),
			Row.of(9, 4, 1.0),
			Row.of(2, 8, 2.2),
			Row.of(2, 7, 2.2),
			Row.of(5, 6, 2.2),
			Row.of(5, 6, 2.2),
			Row.of(7, 5, 2.2),
			Row.of(1, 8, 2.2),
			Row.of(1, 9, 2.2),
			Row.of(7, 5, 2.2),
			Row.of(9, 5, 2.2),
			Row.of(9, 4, 2.2),
		};

		MemSourceBatchOp source = new MemSourceBatchOp(rows, new String[] {"id1", "id2", "value2"});

		OverWindowBatchOp batchWindow = new OverWindowBatchOp()
			.setGroupCols("value2")
			.setOrderBy("id1 asc, id2 desc")
			.setClause("rank() as rank_data, dense_rank() as dense_rank_data,"
				+ " row_number() as row_number_data");
		batchWindow.linkFrom(source).print();
	}

	@Test
	public void testPartition() throws Exception {
		Row[] rows = new Row[] {
			Row.of(2, 8, 1.0),
			Row.of(2, 7, 1.0),
			Row.of(5, 6, 1.0),
			Row.of(5, 6, 1.0),
			Row.of(7, 5, 1.0),
			Row.of(1, 9, 1.0),
			Row.of(1, 9, 1.0),
			Row.of(7, 5, 1.0),
			Row.of(9, 5, 1.0),
			Row.of(9, 4, 1.0),
			Row.of(2, 8, 2.2),
			Row.of(2, 7, 2.2),
			Row.of(5, 6, 2.2),
			Row.of(5, 6, 2.2),
			Row.of(7, 5, 2.2),
			Row.of(1, 8, 2.2),
			Row.of(1, 9, 2.2),
			Row.of(7, 5, 2.2),
			Row.of(9, 5, 2.2),
			Row.of(9, 4, 2.2),
		};

		MemSourceBatchOp source = new MemSourceBatchOp(rows, new String[] {"id1", "id2", "value2"});

		OverWindowBatchOp batchWindow = new OverWindowBatchOp()
			.setGroupCols("value2")
			.setOrderBy("id2 desc")
			.setClause("rank() as rank_data, dense_rank() as dense_rank_data,"
				+ " row_number() as row_number_data");
		batchWindow.linkFrom(source).print();
	}

	@Test
	public void t() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 1L, 1.0f, 5.0, "1.0", Timestamp.valueOf("2017-11-11 10:01:00")),
			Row.of(1, 2L, 2.0f, 4.0, "2.0", Timestamp.valueOf("2017-11-11 10:02:00")),
			Row.of(2, 3L, 3.0f, 3.0, "3.0", Timestamp.valueOf("2017-11-11 10:03:00")),
			Row.of(2, 4L, 4.0f, 4.0, "4.0", Timestamp.valueOf("2017-11-11 10:04:00")),
			Row.of(2, 5L, 5.0f, 5.0, "5.0", Timestamp.valueOf("2017-11-11 10:05:00")),
			Row.of(2, 6L, 6.0f, 6.0, "6.0", Timestamp.valueOf("2017-11-11 10:06:00")),
			Row.of(7, 7L, 7.0f, 7.0, "7.0", Timestamp.valueOf("2017-11-11 10:07:00")),
			Row.of(7, 8L, null, 8.0, "8.0", Timestamp.valueOf("2017-11-11 10:08:00")),
			Row.of(8, 5L, null, null, null, Timestamp.valueOf("2017-11-11 10:09:00")),
			Row.of(9, 999L, 9.0f, 9.0, "9.0", Timestamp.valueOf("2017-11-11 10:10:00"))
		};
		BatchOperator <?> source = new MemSourceBatchOp(rows,
			new String[] {"f_i", "f_l", "f_f", "f_d", "f_s", "timeCol"});

		String operator = "last_distinct";
		String clause =
			operator + "(f_i, f_i) as " + operator + "_f_i, " +
				operator + "(f_l, f_i) as " + operator + "_f_l, " +
				operator + "(f_d, f_i) as " + operator + "_f_d, " +
				operator + "(f_f, f_i) as " + operator + "_f_f, " +
				operator + "(f_s, f_i) as " + operator + "_f_s";

		OverWindowBatchOp batchWindow = new OverWindowBatchOp()
			.setOrderBy("f_i asc")
			.setClause(clause)
			.linkFrom(source);

		System.out.println(batchWindow.getSchema());
		batchWindow.print();
	}

}