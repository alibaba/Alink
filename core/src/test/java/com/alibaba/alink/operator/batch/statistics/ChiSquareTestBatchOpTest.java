package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResults;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;

public class ChiSquareTestBatchOpTest extends AlinkTestBase {

	@Test
	public void test() {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1, 1.2),
				Row.of("b", -2, 0.9, 1.0),
				Row.of("c", 100, -0.01, 1.0),
				Row.of("d", -99, 100.9, 0.1),
				Row.of("a", 1, 1.1, 1.2),
				Row.of("b", -2, 0.9, 1.0),
				Row.of("c", 100, -0.01, 0.2),
				Row.of("d", -99, 100.9, 0.3)
			};

		String[] colNames = new String[] {"col1", "col2", "col3", "col4"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		ChiSquareTestBatchOp test = new ChiSquareTestBatchOp()
			.setSelectedCols("col3", "col1")
			.setLabelCol("col2");

		test.linkFrom(source);

		test.lazyCollectChiSquareTest(new Consumer <ChiSquareTestResults>() {
			@Override
			public void accept(ChiSquareTestResults summary) {
				Assert.assertEquals(summary.results[0].getP(), 0.004301310843500827, 10e-4);
				Assert.assertEquals("col3", summary.results[0].getColName());
			}
		});

		test.lazyPrintChiSquareTest();

		Assert.assertEquals(test.collectChiSquareTest().results[0].getP(), 0.004301310843500827, 10e-4);
	}
}