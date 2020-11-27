package com.alibaba.alink.operator.common.distance;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for FastDistanceVectorData.
 */
public class FastDistanceVectorDataTest {

	@Test
	public void cloneTest() {
		EuclideanDistance distance = new EuclideanDistance();

		FastDistanceVectorData vectorData = new FastDistanceVectorData(DenseVector.rand(10), Row.of(1));
		distance.updateLabel(vectorData);

		FastDistanceVectorData vectorDataClone = new FastDistanceVectorData(vectorData);

		Assert.assertEquals(vectorData.vector, vectorDataClone.vector);
		Assert.assertNotSame(vectorData.vector, vectorDataClone.vector);
		Assert.assertEquals(vectorData.label, vectorDataClone.label);
		Assert.assertNotSame(vectorData.label, vectorDataClone.label);
		Assert.assertArrayEquals(vectorData.rows, vectorDataClone.rows);
		Assert.assertNotSame(vectorData.rows, vectorDataClone.rows);

		vectorData = new FastDistanceVectorData(new SparseVector(10), Row.of(1));
		distance.updateLabel(vectorData);
		vectorDataClone = new FastDistanceVectorData(vectorData);
		Assert.assertEquals(vectorData.vector, vectorDataClone.vector);
		Assert.assertNotSame(vectorData.vector, vectorDataClone.vector);
		Assert.assertEquals(vectorData.label, vectorDataClone.label);
		Assert.assertNotSame(vectorData.label, vectorDataClone.label);
		Assert.assertArrayEquals(vectorData.rows, vectorDataClone.rows);
		Assert.assertNotSame(vectorData.rows, vectorDataClone.rows);
	}

	@Test
	public void jsonTest() {
		EuclideanDistance distance = new EuclideanDistance();

		FastDistanceVectorData vectorData = new FastDistanceVectorData(DenseVector.rand(10));
		distance.updateLabel(vectorData);
		Assert.assertEquals(FastDistanceVectorData.fromString(vectorData.toString()).label.size(), 1);
	}
}