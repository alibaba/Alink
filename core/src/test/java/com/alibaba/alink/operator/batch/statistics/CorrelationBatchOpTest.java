package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.params.statistics.HasMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;

public class CorrelationBatchOpTest extends AlinkTestBase {

	@Test
	public void testCorrelation() {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1L, 1, 2.0, true),
				Row.of(null, 2L, 2, -3.0, true),
				Row.of("c", null, null, 2.0, false),
				Row.of("a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		CorrelationBatchOp corr = new CorrelationBatchOp()
			.setSelectedCols(new String[] {"f_double", "f_int", "f_long"})
			.setMethod("PEARSON");

		corr.linkFrom(source);

		corr.lazyPrintCorrelation();

		CorrelationResult corrMat = corr.collectCorrelation();

		System.out.println(corrMat.toString());

		Assert.assertArrayEquals(corrMat.getCorrelationMatrix().getArrayCopy1D(true),
			new double[] {1.0, -1.0, -1.0,
				-1.0, 1.0, 1.0,
				-1.0, 1.0, 1.0},
			10e-4);
	}

	@Test
	public void testSpearman() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1L, 1, 2.0, true),
				Row.of(null, 2L, 2, -3.0, true),
				Row.of("c", null, null, 2.0, false),
				Row.of("a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		CorrelationBatchOp corr = new CorrelationBatchOp()
			.setSelectedCols(new String[] {"f_double", "f_int", "f_long"})
			.setMethod(HasMethod.Method.SPEARMAN);

		corr.linkFrom(source);

		corr.lazyCollectCorrelation(new Consumer <CorrelationResult>() {
			@Override
			public void accept(CorrelationResult summary) {
				Assert.assertArrayEquals(summary.getCorrelationMatrix().getArrayCopy1D(true),
					new double[] {1.0, -0.7181848464596079, -0.7181848464596079,
						-0.7181848464596079, 1.0, 1.0,
						-0.7181848464596079, 1.0, 1.0},
					10e-4);
			}
		});

		BatchOperator.execute();
	}
}