package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SpearmanCorrelationTest extends AlinkTestBase {

	public BatchOperator getBatchTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {1.0, 2.0}),
				Row.of(new Object[] {-1.0, -3.0}),
				Row.of(new Object[] {4.0, 2.0}),
				Row.of(new Object[] {null, null}),
			};

		String[] colNames = new String[] {"f0", "f1"};

		return new MemSourceBatchOp(Arrays.asList(testArray), colNames);
	}

	@Test
	public void calcRank() throws Exception {
		BatchOperator data = getBatchTable();
		DataSet <Row> out = SpearmanCorrelation.calcRank(data.getDataSet(), false, true);

		List <Row> rows = out.collect();

		Assert.assertEquals(4, rows.size());
		for (Row row : rows) {
			int rowId = (int) (long) row.getField(2);
			switch (rowId) {
				case 0:
					Assert.assertEquals(2.0, row.getField(0));
					Assert.assertEquals(2.0, row.getField(1));
					break;
				case 1:
					Assert.assertEquals(1.0, row.getField(0));
					Assert.assertEquals(1.0, row.getField(1));
					break;
				case 2:
					Assert.assertEquals(3.0, row.getField(0));
					Assert.assertEquals(3.0, row.getField(1));
					break;
				case 3:
					Assert.assertEquals(4.0, row.getField(0));
					Assert.assertEquals(4.0, row.getField(1));
					break;
			}
		}

	}

	@Test
	public void calcRank2() throws Exception {
		BatchOperator data = getBatchTable();
		DataSet <Row> out = SpearmanCorrelation.calcRank(data.getDataSet(), true, true);

		List <Row> rows = out.collect();

		Assert.assertEquals(4, rows.size());

		for (Row row : rows) {
			int rowId = (int) (long) row.getField(1);
			switch (rowId) {
				case 0:
					Assert.assertEquals("2.0 2.0", row.getField(0));
					break;
				case 1:
					Assert.assertEquals("1.0 1.0", row.getField(0));
					break;
				case 2:
					Assert.assertEquals("3.0 3.0", row.getField(0));
					break;
				case 3:
					Assert.assertEquals("4.0 4.0", row.getField(0));
					break;
			}
		}
	}
}