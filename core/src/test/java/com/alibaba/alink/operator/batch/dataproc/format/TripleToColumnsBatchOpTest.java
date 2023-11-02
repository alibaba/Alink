package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TripleToColumnsBatchOpTest extends AlinkTestBase {
	@Test
	public void testTripleToColumnsBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, "f1", 1.0),
			Row.of(1, "f2", 2.0),
			Row.of(2, "f1", 4.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "row int, col string, val double");
		BatchOperator <?> op = new TripleToColumnsBatchOp()
			.setTripleRowCol("row")
			.setTripleColumnCol("col")
			.setTripleValueCol("val")
			.setSchemaStr("f1 double, f2 double")
			.linkFrom(data);
		op.print();
	}
}