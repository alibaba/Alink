package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.params.statistics.HasMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class VectorCorrelationBatchOpTest extends AlinkTestBase {

	@Test
	public void test() {

		Row[] testArray =
			new Row[] {
				Row.of("1.0 2.0"),
				Row.of("-1.0 -3.0"),
				Row.of("4.0 2.0"),
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorCorrelationBatchOp corr = new VectorCorrelationBatchOp()
			.setSelectedCol("vec")
			.setMethod("PEARSON");

		corr.linkFrom(source);

		CorrelationResult corrMat = corr.collectCorrelation();
		System.out.println(corrMat.toString());

		Assert.assertArrayEquals(corrMat.getCorrelationMatrix().getArrayCopy1D(true),
			new double[] {1.0, 0.802955068546966,
				0.802955068546966, 1.0},
			10e-4
		);
	}

	@Test
	public void testSpearman() {

		Row[] testArray =
			new Row[] {
				Row.of("1.0 2.0"),
				Row.of("-1.0 -3.0"),
				Row.of("4.0 2.0"),
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorCorrelationBatchOp corr = new VectorCorrelationBatchOp()
			.setSelectedCol("vec")
			.setMethod(HasMethod.Method.SPEARMAN);

		corr.linkFrom(source);

		CorrelationResult corrMat = corr.collectCorrelation();

		Assert.assertArrayEquals(corrMat.getCorrelationMatrix().getArrayCopy1D(true),
			new double[] {1.0, 0.8660254037844378,
				0.8660254037844378, 1.0},
			10e-4
		);
	}

}