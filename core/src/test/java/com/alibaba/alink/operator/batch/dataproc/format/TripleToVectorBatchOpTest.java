package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TripleToVectorBatchOpTest extends AlinkTestBase {
	@Test
	public void testTripleToVectorBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 1, 1.0),
			Row.of(1, 2, 2.0),
			Row.of(2, 1, 4.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "row int, col int, val double");
		BatchOperator <?> op = new TripleToVectorBatchOp()
			.setTripleRowCol("row")
			.setTripleColumnCol("col")
			.setTripleValueCol("val")
			.setVectorCol("vec")
			.setVectorSize(5)
			.linkFrom(data);
		op.print();
	}
}