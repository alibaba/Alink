package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Test for BucketDiscretizerTrain
 */
public class EqualWidthDiscretizerTrainBatchOpTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1L, 1.1),
				Row.of("b", -2L, 0.9),
				Row.of("c", 100L, -0.01),
				Row.of("d", -99L, 100.9),
				Row.of("a", 1L, 1.1),
				Row.of("b", -2L, 0.9),
				Row.of("c", 100L, -0.01),
				Row.of("d", -99L, 100.9)
			};

		String[] colnames = new String[] {"col1", "col2", "col3"};
		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		EqualWidthDiscretizerTrainBatchOp qop = new EqualWidthDiscretizerTrainBatchOp()
			.setNumBuckets(5)
			.setSelectedCols(colnames[0], colnames[1], colnames[2])
			.linkFrom(sourceBatchOp);

		EqualWidthDiscretizerPredictBatchOp qpop = new EqualWidthDiscretizerPredictBatchOp()
			.setSelectedCols(colnames[1], colnames[2])
			.linkFrom(qop, sourceBatchOp);

		qop.lazyPrint(-1);
		qpop.print();
//		Assert.assertEquals(qpop.select(colnames[1]).distinct().count(), 3);
//		Assert.assertEquals(qpop.select(colnames[2]).distinct().count(), 2);
//
//		thrown.expect(RuntimeException.class);
//		qop.setNumBuckets(5).setNumBucketsArray(5, 6).linkFrom(sourceBatchOp);
	}

}