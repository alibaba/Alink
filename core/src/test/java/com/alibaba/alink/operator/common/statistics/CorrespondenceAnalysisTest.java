package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.statistics.CorrespondenceAnalysis;
import com.alibaba.alink.operator.common.statistics.CorrespondenceAnalysisResult;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CorrespondenceAnalysisTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("a1", "b1"));
		rows.add(Row.of("a2", "b2"));
		rows.add(Row.of("a1", "b2"));
		rows.add(Row.of("a2", "b2"));
		rows.add(Row.of("a1", "b2"));
		rows.add(Row.of("a2", "b2"));
		rows.add(Row.of("a1", "b2"));
		rows.add(Row.of("a2", "b1"));
		rows.add(Row.of("a1", "b1"));

		String[] colNames = new String[] {"f0", "f1"};

		CorrespondenceAnalysisResult result =
			CorrespondenceAnalysis.calc(rows, colNames[0], colNames[1], colNames);

		Assert.assertArrayEquals(
			new double[][] {{0.11180339887498951, 0.0}, {-0.2236067977499789, 0.0}},
			result.colPos);

		Assert.assertArrayEquals(
			new double[][] {{-0.14142135623730945, 0.0}, {0.17677669529663684, 0.0}},
			result.rowPos);

		try {
			CorrespondenceAnalysis.calc(new double[][] {{1.0}});
		} catch (Exception ex) {
			Assert.assertEquals("(the number of column expr) * ( number of row expr) must Greater than 2.!",
				ex.getMessage());
		}
	}

}