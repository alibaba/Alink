package com.alibaba.alink.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for MTable.
 */
public class MTableTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "2", 0, new Timestamp(20000031232012L),
			null,
			new SparseVector(3, new int[] {1}, new double[] {2.0}),
			new FloatTensor(new float[] {3.0f})));

		rows.add(Row.of(null, "2", 0, new Timestamp(1000000234234L),
			new DenseVector(new double[] {0.0, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}),
			new FloatTensor(new float[] {3.0f})));

		MTable mTable = new MTable(
			rows,
			"col0 int, col1 string, label int, ts timestamp"
				+ ", d_vec DENSE_VECTOR"
				+ ", s_vec VECTOR"
				+ ", tensor FLOAT_TENSOR");

		System.out.println(mTable);
		System.out.println(new SparseVector(3, new int[] {1}, new double[] {2.0}).toString());
		System.out.println(new DenseVector(new double[] {0.0, 1.0}).toString());
		System.out.println(MTable.fromJson(JsonConverter.toJson(mTable)));

		List <Object> objects = MTableUtil.getColumn(mTable, "col0");
		System.out.println(objects);

		mTable.orderBy("col1", "ts");

		System.out.println(mTable);

		mTable.orderBy("col0");

		System.out.println(mTable);

		mTable.orderBy("d_vec");

		System.out.println(mTable);
	}

	@Test
	public void testFormat() throws Exception {

		final FloatTensor A = new FloatTensor(
			new float[][][] {
				new float[][] {
					new float[] {0.f, 1.f, 0.1f},
					new float[] {1.f, 1.f, 1.1f},
					new float[] {2.f, 1.f, 2.1f}
				},
				new float[][] {
					new float[] {3.f, 1.f, 3.1f},
					new float[] {4.f, 1.f, 4.1f},
					new float[] {5.f, 1.f, 5.1f}
				},
				new float[][] {
					new float[] {6.f, 1.f, 6.1f},
					new float[] {7.f, 1.f, 7.1f},
					new float[] {8.f, 1.f, 8.1f}
				},
			}
		);
		System.out.println(A.toString());
		List <Row> rows = new ArrayList <>();
		List <Row> rows1 = new ArrayList <>();
		Row row1 = Row.of(1, "2", 0, new Timestamp(20000031232012L),
			null, new SparseVector(3, new int[] {1}, new double[] {2.0}));

		Row row2 = Row.of(null, "2", 0, new Timestamp(1000000234234L),
			new DenseVector(new double[] {0.0, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}));

		Row row3 = Row.of(null, "2", 0, new Timestamp(1000000234234L),
			new DenseVector(new double[] {0.0, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}));
		for (int i = 0; i < 3; ++i) {
			rows.add(row1);
			rows.add(row2);
			rows.add(row3);
		}
		rows1.add(row1);
		rows1.add(row2);
		rows1.add(row3);

		MTable mTable = new MTable(
			rows,
			"col0 int, col1 string, label int, ts timestamp"
				+ ", d_vec DENSE_VECTOR"
				+ ", s_vec VECTOR");
		System.out.println(mTable);

		MTable mTable1 = new MTable(
			rows1,
			"col0 int, col1 string, label int, ts timestamp"
				+ ", d_vec DENSE_VECTOR"
				+ ", s_vec VECTOR");

		MTable mTable2 = new MTable(
			Collections.singletonList(Row.of(mTable, mTable1)),
			"col0 MTABLE, col1 MTABLE"
		);

		Row row = Row.of(1, 3, 4, mTable, "d4353", "dfadsfa", mTable1, "weidosdjaslje", 343, A, mTable2);
		Row row11 = Row.of(1, 3, 4, mTable, "d4353", "dfaderwerewrwesfa", mTable1, "weidosdjaslje", 343, A, mTable2);
		Row row22 = Row.of(1332323, 3, 4, mTable, "d4erwer353", "dfadsfa", mTable1, "weidoeeeeesdjaslje", 343, A,
			mTable2);
		Row row33 = Row.of(1, 333333, 4, mTable, "d4353", "dfadeesfa", mTable1, "weidoseeeedjaslje", 343, A, mTable2);

		Row[] allRows = new Row[] {row, row11, row22, row33};
		new MemSourceBatchOp(allRows,
			new String[] {"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"}).print();

		new MemSourceBatchOp(
			new DoubleTensor[][] {
				{new DoubleTensor(new double[][] {{1.0, 2.0}, {1.0, 2.0}, {3.0, 4.0}}),
					new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}})},
				{new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}}),
					new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}})},
				{new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}}),
					new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}})},
				{new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}}),
					new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}})},
				{new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}}),
					new DoubleTensor(new double[][] {{1.0, 2.0}, {3.0, 4.0}})},
				{new DoubleTensor(new double[][] {{5.0, 6.0}, {7.0, 8.0}}),
					new DoubleTensor(new double[][] {{5.0, 6.0}, {7.0, 8.0}})}
			},
			new String[] {"tensor", "t1"})
			.lazyPrint();

		BatchOperator.execute();
	}

	@Test
	public void testNull() throws Exception {
		BatchOperator.setParallelism(1);
		String schemaStr = TableUtil.schema2SchemaStr(
			new TableSchema(new String[] {"col_mtable"}, new TypeInformation <?>[] {AlinkTypes.STRING})
		);
		List <Row> result = new MemSourceBatchOp(
			new Row[] {
				Row.of(
					new MTable((List <Row>) null, (String) null),
					new MTable((List <Row>) null, schemaStr),
					new MTable(Collections.emptyList(), (String) null),
					new MTable(new Row[] {}, schemaStr)
				)
			},
			new String[] {"col0", "col1", "col2", "col3"}
		).collect();

		Assert.assertTrue(result != null && result.size() == 1);

		Row resultRow = result.get(0);

		Assert.assertNull(((MTable) resultRow.getField(0)).getRows());
		Assert.assertNull(((MTable) resultRow.getField(0)).getSchemaStr());
		Assert.assertNull(((MTable) resultRow.getField(1)).getRows());
		Assert.assertEquals(schemaStr, ((MTable) resultRow.getField(1)).getSchemaStr());
		Assert.assertNotNull(((MTable) resultRow.getField(2)).getRows());
		Assert.assertTrue(((MTable) resultRow.getField(2)).getRows().isEmpty());
		Assert.assertNull(((MTable) resultRow.getField(2)).getSchemaStr());
		Assert.assertNotNull(((MTable) resultRow.getField(3)).getRows());
		Assert.assertTrue(((MTable) resultRow.getField(3)).getRows().isEmpty());
		Assert.assertEquals(schemaStr, ((MTable) resultRow.getField(3)).getSchemaStr());
	}

	@Test
	public void testSummary() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "a", 0.1, -1));
		rows.add(Row.of(2, "b", 0.5, 3));

		String col1 = "col1";
		String col2 = "col2";
		String col3 = "col3";
		double delta = 1e-19;
		MTable mTable = new MTable(rows, "col0 int, col1 string, col2 double, col3 int");

		TableSummary summary1 = mTable.summary();
		TableSummary summary2 = mTable.summary("col1", "col2");
		TableSummary summary3 = mTable.summary("col1", "col3");

		Assert.assertEquals(summary1.mean(col2), summary2.mean(col2), delta);
		Assert.assertEquals(summary1.mean(col3), summary3.mean(col3), delta);
		Assert.assertEquals(summary1.sum(col2), summary2.sum(col2), delta);
		Assert.assertEquals(summary1.sum(col3), summary3.sum(col3), delta);
		Assert.assertEquals(summary1.standardDeviation(col2), summary2.standardDeviation(col2), delta);

		Assert.assertEquals(summary1.sum(col1), Double.NaN, delta);
		Assert.assertEquals(summary2.sum(col1), Double.NaN, delta);
		Assert.assertEquals(summary3.sum(col1), Double.NaN, delta);
	}

	@Test
	public void linkFrom() throws Exception {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "2", 0));
		rows.add(Row.of(1, "2", 0));

		MTable mTable = new MTable(rows, "col0 int, col1 string, label int");
		List <Row> table = new ArrayList <>();
		table.add(Row.of(mTable));

		BatchOperator <?> op = new MemSourceBatchOp(table, new String[] {"mTable"});
		BatchOperator out = op.link(new MTableTestBatchOp());
		List <Row> outRows = out.collect();
		for (Row row : outRows) {
			System.out.println(row);
		}
	}

	@Internal
	class MTableTestBatchOp extends BatchOperator <MTableTestBatchOp> {
		@Override
		public MTableTestBatchOp linkFrom(BatchOperator <?>... inputs) {
			BatchOperator <?> op = checkAndGetFirst(inputs);
			DataSet <Row> data = op.getDataSet();
			DataSet <Row> ret = data.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					MTable mTable = MTable.fromJson(JsonConverter.toJson(value.getField(0)));
					return Row.of(mTable);
				}
			});
			this.setOutput(ret, new TableSchema(new String[] {"out"}, new TypeInformation[] {AlinkTypes.M_TABLE}));
			return this;
		}
	}
}