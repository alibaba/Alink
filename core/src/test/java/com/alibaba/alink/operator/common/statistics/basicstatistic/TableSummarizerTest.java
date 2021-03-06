package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for TableSummarizer.
 */

public class TableSummarizerTest extends AlinkTestBase {

	@Test
	public void testVisitNotCov() {
		TableSummarizer summarizer = testVisit(false);
		System.out.println(summarizer.copy().toString());

		Assert.assertEquals(4, summarizer.count);
		Assert.assertArrayEquals(new double[] {3.0, 3.0, 1.0}, summarizer.sum.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {5.0, 5.0, 17.0}, summarizer.squareSum.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {0.0, 0.0, -3.0}, summarizer.min.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {2.0, 2.0, 2.0}, summarizer.max.getData(), 10e-4);
	}

	@Test
	public void testVisitWithCov() {
		TableSummarizer summarizer = testVisit(true);

		Assert.assertArrayEquals(new double[] {5.0, 5.0, -4.0,
			0.0, 5.0, -4.0,
			0.0, 0.0, 17.0}, summarizer.outerProduct.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {3.0, 3.0, 3.0,
			3.0, 3.0, 3.0,
			-1.0, -1.0, 1}, summarizer.xSum.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {5.0, 5.0, 5.0,
			5.0, 5.0, 5.0,
			13.0, 13.0, 17.0}, summarizer.xSquareSum.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {3.0, 3.0, 2.0,
			3.0, 3.0, 2.0,
			2.0, 2.0, 3.0}, summarizer.xyCount.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, -2.5, -2.5, 8.333333333333334, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -1.0, Double.NaN,
				Double.NaN, 1.0, 1.0, -1.0, Double.NaN,
				Double.NaN, -1.0, -1.0, 1.0, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.correlation().correlation.getArrayCopy1D(true),
			10e-4);

	}

	@Test
	public void testMerge() {
		TableSummarizer summarizer = testWithMerge(false);

		Assert.assertEquals(4, summarizer.count);
		Assert.assertArrayEquals(new double[] {3.0, 3.0, 1.0}, summarizer.sum.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {5.0, 5.0, 17.0}, summarizer.squareSum.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {0.0, 0.0, -3.0}, summarizer.min.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {2.0, 2.0, 2.0}, summarizer.max.getData(), 10e-4);

	}

	@Test
	public void testMergeWithCov() {
		TableSummarizer summarizer = testWithMerge(true);

		Assert.assertArrayEquals(new double[] {5.0, 5.0, -4.0,
			0.0, 5.0, -4.0,
			0.0, 0.0, 17.0}, summarizer.outerProduct.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, -2.5, -2.5, 8.333333333333334, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -1.0, Double.NaN,
				Double.NaN, 1.0, 1.0, -1.0, Double.NaN,
				Double.NaN, -1.0, -1.0, 1.0, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.correlation().correlation.getArrayCopy1D(true),
			10e-4);

	}

	private Row[] geneData() {
		return
			new Row[] {
				Row.of("a", 1L, 1, 2.0, true),
				Row.of(null, 2L, 2, -3.0, true),
				Row.of("c", null, null, 2.0, false),
				Row.of("a", 0L, 0, null, null),
			};
	}

	private TableSummarizer testVisit(boolean bCov) {
		Row[] data = geneData();
		int[] numberIdxs = new int[] {1, 2, 3};
		String[] selectedColNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		TableSummarizer summarizer = new TableSummarizer(selectedColNames, numberIdxs, bCov);
		for (Row aData : data) {
			summarizer.visit(aData);
		}

		return summarizer;
	}

	private TableSummarizer testWithMerge(boolean bCov) {
		Row[] data = geneData();
		int[] numberIdxs = new int[] {1, 2, 3};
		String[] selectedColNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		TableSummarizer summarizerLeft = new TableSummarizer(selectedColNames, numberIdxs, bCov);
		for (int i = 0; i < 2; i++) {
			summarizerLeft.visit(data[i]);
		}

		TableSummarizer summarizerRight = new TableSummarizer(selectedColNames, numberIdxs, bCov);
		for (int i = 2; i < 4; i++) {
			summarizerRight.visit(data[i]);
		}

		return TableSummarizer.merge(summarizerLeft, summarizerRight);
	}
}