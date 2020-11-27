package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorSliceStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class VectorSliceTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("3.0, 4.0, 5.0")
		};
		BatchOperator batchData = new MemSourceBatchOp(rows, new String[] {"vec"});
		StreamOperator streamData = new MemSourceStreamOp(rows, new String[] {"vec"});
		new VectorSliceBatchOp().setSelectedCol("vec").setReservedCols("vec")
			.setIndices(new int[] {0, 1}).linkFrom(batchData).collect();
		new VectorSliceStreamOp().setSelectedCol("vec").setReservedCols("vec")
			.setIndices(new int[] {0, 1}).linkFrom(streamData).print();
		StreamOperator.execute();
	}
}
