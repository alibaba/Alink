package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class VectorSummarizerUtilTest extends AlinkTestBase {
	@Test
	public void test() {
		DenseVectorSummarizer left = new DenseVectorSummarizer(true);
		DenseVectorSummarizer right = new DenseVectorSummarizer(true);
		Assert.assertEquals(0, VectorSummarizerUtil.merge(left, right).count);

		right.visit(new DenseVector(new double[] {1.0}));
		Assert.assertEquals(1, VectorSummarizerUtil.merge(left, right).count);

		left.visit(new DenseVector(new double[] {1.0}));
		Assert.assertEquals(3, VectorSummarizerUtil.merge(left, right).count);
	}

	@Test
	public void testSparse() {
		SparseVectorSummarizer left = new SparseVectorSummarizer(true);
		SparseVectorSummarizer right = new SparseVectorSummarizer(true);
		Assert.assertEquals(0, VectorSummarizerUtil.merge(left, right).count);

		right.visit(new SparseVector(2, new int[] {0}, new double[] {1.0}));
		Assert.assertEquals(1, VectorSummarizerUtil.merge(left, right).count);

		left.visit(new SparseVector(2, new int[] {0}, new double[] {1.0}));
		Assert.assertEquals(3, VectorSummarizerUtil.merge(left, right).count);
	}

	@Test
	public void testMulti() {
		SparseVectorSummarizer left = new SparseVectorSummarizer(true);
		DenseVectorSummarizer right = new DenseVectorSummarizer(true);
		Assert.assertEquals(0, VectorSummarizerUtil.merge(left, right).count);

		right = (DenseVectorSummarizer) right.visit(new DenseVector(new double[] {1.0}));
		Assert.assertEquals(1, VectorSummarizerUtil.merge(left, right).count);

		left = (SparseVectorSummarizer) left.visit(new SparseVector(2, new int[] {0}, new double[] {1.0}));
		Assert.assertEquals(3, VectorSummarizerUtil.merge(left, right).count);
	}

}