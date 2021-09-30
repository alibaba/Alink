package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class GroupDataBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		String[] colnames = new String[] {"id", "id2", "c0", "c1"};
		Row[] testArray =
			new Row[] {
				Row.of("a","a", 1.3, 1.1),
				Row.of("b","a",  -2.5, 0.9),
				Row.of("c","a",  100.2, -0.01),
				Row.of("d","a",  -99.9, 100.9),
				Row.of("a","a",  1.4, 1.1),
				Row.of("b","a",  -2.2, 0.9),
				Row.of("c","a",  100.9, -0.01),
				Row.of("d","a",  -99.5, 100.9)
			};
		BatchOperator in = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		new GroupDataBatchOp().setGroupCols("id", "id2").setSelectedCols("id", "c0", "c1").setOutputCol("mTable")
			.linkFrom(in).print();
	}
}