package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorSummaryDataConverterTest extends AlinkTestBase {

	@Test
	public void testDense() {
		VectorSummaryDataConverter converter = new VectorSummaryDataConverter();
		DenseVectorSummary denseSummary = denseSummary();

		List <Row> rowList = new ArrayList <>();
		ListCollector <Row> rows = new ListCollector(rowList);

		converter.save(denseSummary, rows);
		DenseVectorSummary newSummary = (DenseVectorSummary) converter.load(rowList);

		Assert.assertArrayEquals(denseSummary.sum.getData(), newSummary.sum.getData(), 10e-8);

		Assert.assertNull(converter.serializeModel(null));
		Assert.assertNull(converter.deserializeModel(null, null));
	}

	@Test
	public void testSparse() {
		VectorSummaryDataConverter converter = new VectorSummaryDataConverter();
		SparseVectorSummary sparseSummary = sparseSummary();

		List <Row> rowList = new ArrayList <>();
		ListCollector <Row> rows = new ListCollector(rowList);

		converter.save(sparseSummary, rows);
		SparseVectorSummary newSummary = (SparseVectorSummary) converter.load(rowList);

		Assert.assertEquals(sparseSummary.sum(0), newSummary.sum(0), 10e-8);
	}

	private DenseVectorSummary denseSummary() {
		DenseVector[] data = new DenseVector[] {
			new DenseVector(new double[] {1.0, -1.0, 3.0}),
			new DenseVector(new double[] {2.0, -2.0, 3.0}),
			new DenseVector(new double[] {3.0, -3.0, 3.0}),
			new DenseVector(new double[] {4.0, -4.0, 3.0}),
			new DenseVector(new double[] {5.0, -5.0, 3.0})
		};
		;
		DenseVectorSummarizer summarizer = new DenseVectorSummarizer();
		for (DenseVector aData : data) {
			summarizer.visit(aData);
		}
		return (DenseVectorSummary) summarizer.toSummary();
	}

	private SparseVectorSummary sparseSummary() {
		SparseVector[] data = new SparseVector[] {
			new SparseVector(5, new int[] {0, 1, 2}, new double[] {1.0, -1.0, 3.0}),
			new SparseVector(5, new int[] {1, 2, 3}, new double[] {2.0, -2.0, 3.0}),
			new SparseVector(5, new int[] {2, 3, 4}, new double[] {3.0, -3.0, 3.0}),
			new SparseVector(5, new int[] {0, 2, 3}, new double[] {4.0, -4.0, 3.0}),
			new SparseVector(5, new int[] {0, 1, 4}, new double[] {5.0, -5.0, 3.0})
		};
		SparseVectorSummarizer summarizer = new SparseVectorSummarizer();
		for (SparseVector aData : data) {
			summarizer.visit(aData);
		}
		return (SparseVectorSummary) summarizer.toSummary();
	}

}