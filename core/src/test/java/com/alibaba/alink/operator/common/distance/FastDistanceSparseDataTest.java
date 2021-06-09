package com.alibaba.alink.operator.common.distance;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Unit test for FastDistanceSparseData.
 */

public class FastDistanceSparseDataTest extends AlinkTestBase {
	private List <Integer>[] indices;
	private List <Double>[] values;
	private HashMap <Integer, Tuple2 <List <Integer>, List <Double>>> indexHashMap;

	@Before
	public void init() {
		indices = new ArrayList[5];
		values = new ArrayList[5];
		indexHashMap = new HashMap <>();
		for (int i = 0; i < 5; i++) {
			indices[i] = new ArrayList <>();
			values[i] = new ArrayList <>();
			indices[i].add(i);
			indices[i].add(i + 1);
			indices[i].add(i + 2);
			values[i].add(1.0 * i);
			values[i].add(i * 2.);
			values[i].add(i * 3.);
			indexHashMap.put(i, Tuple2.of(indices[i], values[i]));
		}
	}

	@Test
	public void cloneTest() {
		EuclideanDistance distance = new EuclideanDistance();

		FastDistanceSparseData matrixData = new FastDistanceSparseData(indices, values, 20);
		distance.updateLabel(matrixData);

		FastDistanceSparseData vectorDataClone = new FastDistanceSparseData(matrixData);

		Assert.assertEquals(matrixData.indices, vectorDataClone.indices);
		Assert.assertNotSame(matrixData.values, vectorDataClone.values);
		Assert.assertEquals(matrixData.label, vectorDataClone.label);
		Assert.assertNotSame(matrixData.label, vectorDataClone.label);

		matrixData = new FastDistanceSparseData(indexHashMap, 20);
		distance.updateLabel(matrixData);

		vectorDataClone = new FastDistanceSparseData(matrixData);

		Assert.assertEquals(matrixData.indices, vectorDataClone.indices);
		Assert.assertNotSame(matrixData.values, vectorDataClone.values);
		Assert.assertEquals(matrixData.label, vectorDataClone.label);
		Assert.assertNotSame(matrixData.label, vectorDataClone.label);
	}

	@Test
	public void jsonTest() {
		EuclideanDistance distance = new EuclideanDistance();

		FastDistanceSparseData matrixData = new FastDistanceSparseData(indices, values, 20);
		distance.updateLabel(matrixData);
		Assert.assertEquals(FastDistanceSparseData.fromString(matrixData.toString()).getLabel().numCols(), 20);
	}
}