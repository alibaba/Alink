package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.pipeline.dataproc.format.VectorToColumns;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class VectorToColumnsBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 2, "$5$1:2 4:5")
		};
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"id", "id2", "vec"});

		VectorToColumnsBatchOp newOp = new VectorToColumnsBatchOp()
			.setVectorCol("vec").setSchemaStr("f0 double, f1 double, f2 double, f3 double")
			.setReservedCols("id");
		newOp.linkFrom(data).print();
	}

	@Test
	public void testPipeline() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 2, "$5$1:2 3:5")
		};
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"id", "id2", "vec"});
		VectorToColumns pipeline = new VectorToColumns()
			.setVectorCol("vec").setSchemaStr("f0 double, f1 double, f2 double, f3 double, f4 double")
			.setReservedCols("id", "id2");
		pipeline.transform(data).print();
	}

}