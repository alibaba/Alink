package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class GmmTrainBatchOpTest extends AlinkTestBase {

	@Test
	public void testRandomData() throws Exception {
		Row[] rows = new Row[] {
			Row.of("0 0 0"),
			Row.of("0.1 0.1 0.1"),
			Row.of("0.2 0.2 0.2"),
			Row.of("0.3 0.3 0.3"),
			Row.of("0.4 0.4 0.4"),
			Row.of("9 9 9"),
			Row.of("9.1 9.1 9.1"),
			Row.of("9.2 9.2 9.2"),
			Row.of("9.3 9.3 9.3"),
			Row.of("9.4 9.4 9.4")
		};
		BatchOperator source = new MemSourceBatchOp(rows, new String[] {"vector"});
		GmmTrainBatchOp op = new GmmTrainBatchOp()
			.setVectorCol("vector")
			.setK(2)
			.setMaxIter(30)
			.linkFrom(source);
		op.print();
	}

}
