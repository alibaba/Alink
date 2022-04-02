package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for gbdt.
 */

public class FlattenMTableTest extends AlinkTestBase {

	@Test
	public void test1() throws Exception {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "2021-01-09 00:00:00", 0,
			null,
			new SparseVector(3, new int[] {1}, new double[] {2.0}),
			new FloatTensor(new float[] {3.0f})));

		rows.add(Row.of(null, "2021-01-09 00:00:00", 0,
			new DenseVector(new double[] {0.0, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}),
			new FloatTensor(new float[] {3.0f})));

		rows.add(Row.of(null, "2021-01-09 00:00:00", 0,
			new DenseVector(new double[] {0.1, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}),
			new FloatTensor(new float[] {3.0f})));

		String schemaStr = "col0 int, col1 string, label long"
			+ ", d_vec DENSE_VECTOR"
			+ ", s_vec SPARSE_VECTOR"
			+ ", tensor FLOAT_TENSOR";
		MTable mTable = new MTable(rows, schemaStr);
		List <Row> table = new ArrayList <>();
		table.add(Row.of("id", JsonConverter.toJson(mTable)));

		BatchOperator <?> op = new MemSourceBatchOp(table, new String[] {"id", "mTable"});

		String flattenSchemaStr = "col0 double, col1 timestamp, label timestamp"
			+ ", d_vec DENSE_VECTOR"
			+ ", s_vec SPARSE_VECTOR"
			+ ", tensor FLOAT_TENSOR";
		BatchOperator <?> out = op.link(new FlattenMTableBatchOp().setSchemaStr(flattenSchemaStr)
			.setSelectedCol("mTable").setReservedCols("id").setHandleInvalidMethod(HandleInvalidMethod.SKIP));
		List <Row> res = out.collect();
		for (Row row : res) {
			Assert.assertEquals("id", row.getField(0));
		}
	}

	@Test
	public void test2() throws Exception {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("a1", "11L", 2.2));
		rows.add(Row.of("a1", "12L", 2.0));
		rows.add(Row.of("a2", "11L", 2.0));
		rows.add(Row.of("a2", "12L", 2.0));
		rows.add(Row.of("a3", "12L", 2.0));
		rows.add(Row.of("a3", "13L", 2.0));
		rows.add(Row.of("a4", "13L", 2.0));
		rows.add(Row.of("a4", "14L", 2.0));
		rows.add(Row.of("a5", "14L", 2.0));
		rows.add(Row.of("a5", "15L", 2.0));
		rows.add(Row.of("a6", "15L", 2.0));
		rows.add(Row.of("a6", "16L", 2.0));

		BatchOperator <?> input = new MemSourceBatchOp(rows, "id string, f0 string, f1 double");

		GroupByBatchOp zip = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("id, mtable_agg(f0, f1) as m_table_col");

		FlattenMTableBatchOp flatten = new FlattenMTableBatchOp()
			.setReservedCols("id")
			.setSelectedCol("m_table_col")
			.setSchemaStr("f0 string, f1 int");

		List <Row> res = zip.linkFrom(input).link(flatten).collect();
		for (Row row : res) {
			Assert.assertEquals(2, row.getField(2));
		}
	}

	@Test
	public void test3() throws Exception {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("a1", "{\"data\":{\"f0\":[\"11L\",\"12L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 "
			+ "DOUBLE\"}"));
		rows.add(Row.of("a3", "{\"data\":{\"f0\":[\"13L\",\"14L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 "
			+ "DOUBLE\"}"));
		StreamOperator <?> input = new MemSourceStreamOp(rows, "id string, mt string");

		FlattenMTableStreamOp flatten = new FlattenMTableStreamOp()
			.setReservedCols("id")
			.setSelectedCol("mt")
			.setSchemaStr("f0 string, f1 int");

		CollectSinkStreamOp sop = flatten.linkFrom(input).link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List<Row> res = sop.getAndRemoveValues();
		for (Row row : res) {
			Assert.assertEquals(2, row.getField(2));
		}
	}
}