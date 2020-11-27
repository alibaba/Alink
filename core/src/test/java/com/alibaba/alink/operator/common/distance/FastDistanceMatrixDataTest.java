package com.alibaba.alink.operator.common.distance;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for FastDistanceMatrixData.
 */
public class FastDistanceMatrixDataTest {

	@Test
	public void cloneTest() {
		EuclideanDistance distance = new EuclideanDistance();

		FastDistanceMatrixData matrixData = new FastDistanceMatrixData(DenseMatrix.rand(10, 10), new Row[10]);
		distance.updateLabel(matrixData);

		FastDistanceMatrixData vectorDataClone = new FastDistanceMatrixData(matrixData);

		Assert.assertEquals(matrixData.vectors, vectorDataClone.vectors);
		Assert.assertNotSame(matrixData.vectors, vectorDataClone.vectors);
		Assert.assertEquals(matrixData.label, vectorDataClone.label);
		Assert.assertNotSame(matrixData.label, vectorDataClone.label);
		Assert.assertArrayEquals(matrixData.rows, vectorDataClone.rows);
		Assert.assertNotSame(matrixData.rows, vectorDataClone.rows);
	}

	@Test
	public void jsonTest() {
		EuclideanDistance distance = new EuclideanDistance();

		FastDistanceMatrixData matrixData = new FastDistanceMatrixData(DenseMatrix.rand(10, 10));
		distance.updateLabel(matrixData);
		Assert.assertEquals(FastDistanceMatrixData.fromString(matrixData.toString()).getLabel().numRows(), 1);
	}
}