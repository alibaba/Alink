package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.MultiCollinearityBatchOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MultiCollinearityBatchOpTest extends TestCase {
	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"y", "x1", "x2"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()}
		);

		Object[][] data = new Object[][] {
			{16.3, 1.1, 1.1},
			{16.8, 1.4, 1.5},
			{19.2, 1.7, 1.8},
			{18.0, 1.7, 1.7},
			{19.5, 1.8, 1.9},
			{20.9, 1.8, 1.8},
			{21.1, 1.9, 1.8},
			{20.9, 2.0, 2.1},
			{20.3, 2.3, 2.4},
			{22.0, 2.4, 2.5}
		};

		List <Row> rows = new ArrayList <>();

		for (Object[] datarow : data) {
			rows.add(Row.of(datarow[0], datarow[1], datarow[2]));
		}

		MemSourceBatchOp mbts = new MemSourceBatchOp(rows, schema);

		MultiCollinearityBatchOp multi = new MultiCollinearityBatchOp()
			.setSelectedCols("y", "x1", "x2");

		mbts.link(multi).print();
	}
}