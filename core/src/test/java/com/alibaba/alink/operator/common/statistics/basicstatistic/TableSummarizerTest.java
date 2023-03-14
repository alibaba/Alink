package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.statistics.statistics.NumberMeasureIterator;
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
		Assert.assertEquals(3.0, ((NumberMeasureIterator <?>) summarizer.statIterators[1]).sum, 10e-4);
		Assert.assertEquals(3.0, ((NumberMeasureIterator <?>) summarizer.statIterators[2]).sum, 10e-4);
		Assert.assertEquals(1.0, ((NumberMeasureIterator <?>) summarizer.statIterators[3]).sum, 10e-4);
		Assert.assertEquals(5.0, ((NumberMeasureIterator <?>) summarizer.statIterators[1]).sum2, 10e-4);
		Assert.assertEquals(5.0, ((NumberMeasureIterator <?>) summarizer.statIterators[2]).sum2, 10e-4);
		Assert.assertEquals(17.0, ((NumberMeasureIterator <?>) summarizer.statIterators[3]).sum2, 10e-4);
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

		System.out.println(summarizer.covariance());
		System.out.println(summarizer.correlation());

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5,
				Double.NaN, 1.0, 1.0, -2.5,
				Double.NaN, -2.5, -2.5, 8.333333333333334},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -1.0,
				Double.NaN, 1.0, 1.0, -1.0,
				Double.NaN, -1.0, -1.0, 1.0},
			summarizer.correlation().correlation.getArrayCopy1D(true),
			10e-4);

	}

	@Test
	public void testMerge() {
		TableSummarizer summarizer = testWithMerge(false);

		Assert.assertEquals(4, summarizer.count);
		Assert.assertEquals(4, summarizer.count);
		Assert.assertEquals(3.0, ((NumberMeasureIterator <?>) summarizer.statIterators[1]).sum, 10e-4);
		Assert.assertEquals(3.0, ((NumberMeasureIterator <?>) summarizer.statIterators[2]).sum, 10e-4);
		Assert.assertEquals(1.0, ((NumberMeasureIterator <?>) summarizer.statIterators[3]).sum, 10e-4);
		Assert.assertEquals(5.0, ((NumberMeasureIterator <?>) summarizer.statIterators[1]).sum2, 10e-4);
		Assert.assertEquals(5.0, ((NumberMeasureIterator <?>) summarizer.statIterators[2]).sum2, 10e-4);
		Assert.assertEquals(17.0, ((NumberMeasureIterator) summarizer.statIterators[3]).sum2, 10e-4);

	}

	@Test
	public void testMergeWithCov() {
		TableSummarizer summarizer = testWithMerge(true);

		Assert.assertArrayEquals(new double[] {5.0, 5.0, -4.0,
			0.0, 5.0, -4.0,
			0.0, 0.0, 17.0}, summarizer.outerProduct.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5,
				Double.NaN, 1.0, 1.0, -2.5,
				Double.NaN, -2.5, -2.5, 8.333333333333334},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -1.0,
				Double.NaN, 1.0, 1.0, -1.0,
				Double.NaN, -1.0, -1.0, 1.0},
			summarizer.correlation().correlation.getArrayCopy1D(true),
			10e-4);

	}

	private Row[] geneData() {
		return
			new Row[] {
				Row.of("a", 1L, 1, 2.0),
				Row.of(null, 2L, 2, -3.0),
				Row.of("c", null, null, 2.0),
				Row.of("a", 0L, 0, null),
			};
	}

	private TableSummarizer testVisit(boolean bCov) {
		Row[] data = geneData();
		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double"};
		TypeInformation <?>[] colTypes = new TypeInformation <?>[] {
			Types.STRING, Types.LONG, Types.INT, Types.DOUBLE
		};

		TableSchema schema = new TableSchema(colNames, colTypes);

		TableSummarizer summarizer = new TableSummarizer(schema, bCov);
		for (Row aData : data) {
			summarizer.visit(aData);
		}

		return summarizer;
	}

	private TableSummarizer testWithMerge(boolean bCov) {
		Row[] data = geneData();
		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double"};
		TypeInformation <?>[] colTypes = new TypeInformation <?>[] {
			Types.STRING, Types.LONG, Types.INT, Types.DOUBLE
		};

		TableSchema schema = new TableSchema(colNames, colTypes);
		TableSummarizer summarizerLeft = new TableSummarizer(schema, bCov);
		for (int i = 0; i < 2; i++) {
			summarizerLeft.visit(data[i]);
		}

		TableSummarizer summarizerRight = new TableSummarizer(schema, bCov);
		for (int i = 2; i < 4; i++) {
			summarizerRight.visit(data[i]);
		}

		return TableSummarizer.merge(summarizerLeft, summarizerRight);
	}
}