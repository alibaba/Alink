package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class AppendIdBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"2", -2, 0.9, 2.0, false}),
				Row.of(new Object[] {"3", 100, -0.01, 3.0, true}),
				Row.of(new Object[] {"4", -99, null, 4.0, false}),
				Row.of(new Object[] {"5", 1, 1.1, 5.0, true}),
				Row.of(new Object[] {"6", -2, 0.9, 6.0, false})
			};
		String[] colnames = new String[] {"col1", "col2", "col3", "col4", "col5"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		AppendIdBatchOp appendOp = new AppendIdBatchOp();
		inOp.link(appendOp).print();
	}

	@Test
	public void test1() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"2", -2, 0.9, 2.0, false}),
				Row.of(new Object[] {"3", 100, -0.01, 3.0, true}),
				Row.of(new Object[] {"4", -99, null, 4.0, false}),
				Row.of(new Object[] {"5", 1, 1.1, 5.0, true}),
				Row.of(new Object[] {"6", -2, 0.9, 6.0, false})
			};
		String[] colnames = new String[] {"col1", "col2", "col3", "col4", "col5"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		AppendIdBatchOp appendOp = new AppendIdBatchOp().setIdCol("test_id");
		inOp.link(appendOp).print();
	}
}
