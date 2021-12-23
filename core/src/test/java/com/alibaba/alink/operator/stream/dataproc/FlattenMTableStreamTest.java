package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for gbdt.
 */

public class FlattenMTableStreamTest extends AlinkTestBase {

	@Test
	public void linkFrom() throws Exception {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "2", 0,
			null,
			new SparseVector(3, new int[] {1}, new double[] {2.0}),
			new FloatTensor(new float[] {3.0f})));

		rows.add(Row.of(null, "2", 0,
			new DenseVector(new double[] {0.0, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}),
			new FloatTensor(new float[] {3.0f})));

		rows.add(Row.of(null, "2", 0,
			new DenseVector(new double[] {0.1, 1.0}),
			new SparseVector(4, new int[] {2}, new double[] {3.0}),
			new FloatTensor(new float[] {3.0f})));

		String schemaStr = "col0 int, col1 string, label int"
			+ ", d_vec VEC_TYPES_DENSE_VECTOR"
			+ ", s_vec VEC_TYPES_SPARSE_VECTOR"
			+ ", tensor TENSOR_TYPES_FLOAT_TENSOR";
		MTable mTable = new MTable(rows, schemaStr);
		List <Row> table = new ArrayList <>();
		table.add(Row.of("id", mTable.toString()));

		StreamOperator <?> op = new MemSourceStreamOp(table, new String[] {"id","mTable"});
		op.link(new FlattenMTableStreamOp().setSchemaStr(schemaStr)
			.setSelectedCol("mTable").setReservedCols("id")).print();
		StreamOperator.execute();
	}
}