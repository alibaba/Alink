package com.alibaba.alink.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for gbdt.
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
				+ ", d_vec densevector"
				+ ", s_vec vector"
				+ ", tensor TENSOR_TYPES_FLOAT_TENSOR");

		System.out.println(mTable);

		System.out.println(new MTable(mTable.toString()));

		List <Object> objects = MTableUtils.getColumn(mTable, "col0");
		System.out.println(objects);

		mTable.orderBy("col1", "ts");

		System.out.println(mTable);

		mTable.orderBy("col0");

		System.out.println(mTable);

		mTable.orderBy("d_vec");

		System.out.println(mTable);
	}

	@Test
	public void testNull() throws Exception {
		new MemSourceBatchOp(
			new Row[] {
				Row.of(new MTable(null, new String[] {"col_mtable"}, new TypeInformation <?>[] {Types.STRING}))
			},
			new String[] {"col0"}
		).print();
	}

	@Test
	public void linkFrom() throws Exception {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "2", 0));
		rows.add(Row.of(1, "2", 0));

		MTable mTable = new MTable(rows, "col0 int, col1 string, label int");
		List <Row> table = new ArrayList <>();
		table.add(Row.of(mTable.toString()));

		BatchOperator <?> op = new MemSourceBatchOp(table, new String[] {"mTable"});
		BatchOperator out = op.link(new MTableTestBatchOp()).link(new MTableTestBatchOp());
		List<Row> outRows = out.collect();
		for (Row row :outRows) {
			System.out.println(row);
		}
	}

	class MTableTestBatchOp extends BatchOperator <MTableTestBatchOp> {
		@Override
		public MTableTestBatchOp linkFrom(BatchOperator <?>... inputs) {
			BatchOperator <?> op = checkAndGetFirst(inputs);
			DataSet <Row> data = op.getDataSet();
			DataSet <Row> ret = data.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					MTable mTable = new MTable((value.getField(0).toString()));
					return Row.of(mTable);
				}
			});
			this.setOutput(ret, new TableSchema(new String[] {"out"}, new TypeInformation[] {MTableTypes.M_TABLE}));
			return this;
		}
	}
}