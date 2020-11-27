package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class SampleBatchOpTest extends AlinkTestBase {

	public static Table getBatchTable() {
		Row[] testArray =
			new Row[] {
				Row.of(4.0, 2.0),
				Row.of(null, null),
				Row.of(1.0, 2.0),
				Row.of(-1.0, -3.0),
				Row.of(4.0, 2.0),
				Row.of(null, null),
				Row.of(1.0, 2.0),
				Row.of(-1.0, -3.0),
				Row.of(4.0, 2.0),
				Row.of(null, null)
			};
		String[] colNames = new String[] {"f0", "f1"};
		return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
	}

	@Test
	public void test() throws Exception {
		TableSourceBatchOp tableSourceBatchOp = new TableSourceBatchOp(getBatchTable());
		long cnt = tableSourceBatchOp.link(new SampleBatchOp(0.5, true)).count();
		assert cnt >= 0 && cnt <= 10;
	}
}
