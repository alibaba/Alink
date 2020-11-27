package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class StratifiedSampleBatchOpTest extends AlinkTestBase {

	private String[] colnames = new String[] {"col1", "col2", "col3"};

	private MemSourceBatchOp getSourceBatchOp(){
		Row[] testArray =
			new Row[] {
				Row.of("a", 1.3, 1.1),
				Row.of("b", -2.5, 0.9),
				Row.of("c", 100.2, -0.01),
				Row.of("d", -99.9, 100.9),
				Row.of("a", 1.4, 1.1),
				Row.of("b", -2.2, 0.9),
				Row.of("c", 100.9, -0.01),
				Row.of("d", -99.5, 100.9)
			};
		return new MemSourceBatchOp(Arrays.asList(testArray), colnames);
	}

	@Test
	public void test1() throws Exception {
		StratifiedSampleBatchOp stratifiedSampleBatchOp = new StratifiedSampleBatchOp()
			.setStrataCol(colnames[0])
			.setStrataRatios("a:0.5,b:0.5,c:0.5,d:1.0");
		long cnt = getSourceBatchOp().link(stratifiedSampleBatchOp).count();
		assert cnt >= 0 && cnt <= 8;
	}

	@Test
	public void test2() throws Exception {
		StratifiedSampleBatchOp stratifiedSampleBatchOp = new StratifiedSampleBatchOp()
			.setStrataCol(colnames[0])
			.setStrataRatio(0.5)
			.setStrataRatios("a:0.5,b:0.5,c:0.5,d:1.0");
		long cnt = getSourceBatchOp().link(stratifiedSampleBatchOp).count();
		assert cnt >= 0 && cnt <= 8;
	}
}
