package com.alibaba.alink.operator.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class PackBatchOperatorUtilTest extends AlinkTestBase {

	@Test
	public void test() {
		BatchOperator op = pack();
		TableSchema modelSchema = op.getSchema();
		List <Row> modelRows = op.collect();

		testPackBatchOps(modelRows, modelSchema);
		testUnpackRows(modelRows, modelSchema);
		testUnpackSchema(modelRows, modelSchema);

	}

	public void testPackBatchOps(List <Row> rows, TableSchema modelSchema) {
		assertArrayEquals(new String[] {"id", "p0", "p1", "p2", "p3", "p4", "p5"}, modelSchema.getFieldNames());
		assertArrayEquals(new TypeInformation[] {Types.INT, Types.STRING, Types.INT,
				Types.INT, Types.INT, Types.DOUBLE, Types.DOUBLE},
			modelSchema.getFieldTypes());

		assertEquals(7, rows.size());
		TableSummarizer summarizer = new TableSummarizer(modelSchema.getFieldNames(), new int[] {0, 2, 3, 4, 5, 6},
			false);
		for (Row row : rows) {
			summarizer.visit(row);
		}
		TableSummary summary = summarizer.toSummary();
		assertEquals(6, summary.numMissingValue("p0"));
		assertEquals(1, summary.numMissingValue("p1"));
		assertEquals(1, summary.numMissingValue("p2"));
		assertEquals(3, summary.numMissingValue("p3"));
		assertEquals(1, summary.numMissingValue("p4"));
		assertEquals(5, summary.numMissingValue("p5"));
	}

	public void testUnpackRows(List <Row> rows, TableSchema modelSchema) {
		List <Row> row0 = PackBatchOperatorUtil.unpackRows(rows, 0);
		assertEquals(2, row0.size());

		assertEquals("10,11,12,13.0", row0.get(0).toString());
		assertEquals("20,21,22,23.0", row0.get(1).toString());

		List <Row> row1 = PackBatchOperatorUtil.unpackRows(rows, 1);
		assertEquals(2, row1.size());

		assertEquals("30.0,31.0,32,33", row1.get(0).toString());
		assertEquals("40.0,41.0,42,43", row1.get(1).toString());

		List <Row> row2 = PackBatchOperatorUtil.unpackRows(rows, 2);
		assertEquals(2, row2.size());

		assertEquals("50,51,52,53.0", row2.get(0).toString());
		assertEquals("60,61,63,63.0", row2.get(1).toString());
	}

	public void testUnpackSchema(List <Row> rows, TableSchema modelSchema) {
		TableSchema schema0 = PackBatchOperatorUtil.unpackSchema(rows, modelSchema, 0);
		assertArrayEquals(new String[] {"f0", "f1", "f2", "f3"}, schema0.getFieldNames());
		assertArrayEquals(new TypeInformation[] {Types.INT, Types.INT, Types.INT, Types.DOUBLE},
			schema0.getFieldTypes());

		TableSchema schema1 = PackBatchOperatorUtil.unpackSchema(rows, modelSchema, 1);
		assertArrayEquals(new String[] {"f0", "f1", "f2", "f3"}, schema1.getFieldNames());
		assertArrayEquals(new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.INT, Types.INT},
			schema1.getFieldTypes());

		TableSchema schema2 = PackBatchOperatorUtil.unpackSchema(rows, modelSchema, 2);
		assertArrayEquals(new String[] {"f0", "f1", "f2", "f3"}, schema2.getFieldNames());
		assertArrayEquals(new TypeInformation[] {Types.INT, Types.INT, Types.INT, Types.DOUBLE},
			schema2.getFieldTypes());

	}

	BatchOperator pack() {
		List <Row> rowList1 = new ArrayList <>();
		rowList1.add(Row.of(10, 11, 12, 13.0));
		rowList1.add(Row.of(20, 21, 22, 23.0));
		TableSchema schema1 = new TableSchema(new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation[] {Types.INT, Types.INT, Types.INT, Types.DOUBLE});
		MemSourceBatchOp op1 = new MemSourceBatchOp(rowList1, schema1);

		List <Row> rowList2 = new ArrayList <>();
		rowList2.add(Row.of(30.0, 31.0, 32, 33));
		rowList2.add(Row.of(40.0, 41.0, 42, 43));
		TableSchema schema2 = new TableSchema(new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.INT, Types.INT});
		MemSourceBatchOp op2 = new MemSourceBatchOp(rowList2, schema2);

		List <Row> rowList3 = new ArrayList <>();
		rowList3.add(Row.of(50, 51, 52, 53.0));
		rowList3.add(Row.of(60, 61, 63, 63.0));
		TableSchema schema3 = new TableSchema(new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation[] {Types.INT, Types.INT, Types.INT, Types.DOUBLE});
		MemSourceBatchOp op3 = new MemSourceBatchOp(rowList3, schema3);

		return PackBatchOperatorUtil.packBatchOps(new BatchOperator[] {op1, op2, op3});
	}

}