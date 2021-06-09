package com.alibaba.alink.operator.common.distance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for ManHattanDistance
 */

public class ManHattanDistanceTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private ManHattanDistance distance = new ManHattanDistance();
	private DenseVector denseVector1 = new DenseVector(new double[] {-1, 2, 4, 1, 3});
	private DenseVector denseVector2 = new DenseVector(new double[] {4, 6, 1, 2, 4});
	private SparseVector sparseVector1 = new SparseVector(5, new int[] {1, 3}, new double[] {-0.1, 0.4});
	private SparseVector sparseVector2 = new SparseVector(5, new int[] {2, 3}, new double[] {0.4, -0.1});

	@Test
	public void testContinuousDistance() {
		Assert.assertEquals(distance.calc(denseVector1, denseVector2), 14.0, 0.01);
		Assert.assertEquals(distance.calc(denseVector1.getData(), denseVector2.getData()), 14.0, 0.01);
		Assert.assertEquals(distance.calc(denseVector1, sparseVector1), 10.7, 0.01);
		Assert.assertEquals(distance.calc(sparseVector1, sparseVector2), 1.0, 0.01);
		Assert.assertEquals(distance.calc(sparseVector1, denseVector2), 16.7, 0.01);
	}

	private FastDistanceMatrixData initMatrixData() {
		List <Row> dense = new ArrayList <>();
		dense.add(Row.of(0, denseVector1));
		dense.add(Row.of(1, denseVector2));
		return (FastDistanceMatrixData) distance.prepareMatrixData(dense, 1).get(0);
	}

	private FastDistanceSparseData initSparseData() {
		List <Row> sparse = new ArrayList <>();
		sparse.add(Row.of(0, sparseVector1));
		sparse.add(Row.of(1, sparseVector2));
		sparse.add(Row.of(1, sparseVector2));
		return (FastDistanceSparseData) distance.prepareMatrixData(sparse, 1).get(0);
	}

	@Test
	public void testUpdateLabel() {
		DenseVector vector = distance.prepareVectorData(Tuple2.of(denseVector1, null)).getLabel();
		Assert.assertEquals(vector.size(), 1);
		Assert.assertEquals(vector.get(0), 11, 0.1);

		FastDistanceSparseData sparseData = initSparseData();
		Assert.assertEquals(sparseData.label.numCols(), 3);
		Assert.assertEquals(sparseData.label.numRows(), 1);

		double[] labelData = sparseData.label.getData();
		Assert.assertEquals(labelData[0], 0.5, 0.01);
		Assert.assertEquals(labelData[1], 0.5, 0.01);
		Assert.assertEquals(labelData[2], 0.5, 0.01);
	}

	@Test
	public void testCalDistanceVecVec() {
		DenseMatrix denseResult = distance.calc(distance.prepareVectorData(Tuple2.of(denseVector1, null)),
			(FastDistanceData) distance.prepareVectorData(Tuple2.of(denseVector2, null)));
		Assert.assertEquals(denseResult.get(0, 0), 14.0, 0.01);

		DenseMatrix sparseResult = distance.calc(distance.prepareVectorData(Tuple2.of(sparseVector1, null)),
			(FastDistanceData) distance.prepareVectorData(Tuple2.of(

				sparseVector2, null)));
		Assert.assertEquals(sparseResult.get(0, 0), 1.0, 0.01);
	}

	@Test
	public void testCalDistanceMatrixMatrix() {
		FastDistanceMatrixData matrixData = initMatrixData();
		DenseMatrix res = distance.calc(matrixData, matrixData);
		double[] expect = new double[] {0.0, 14.0, 14.0, 0.0};
		double[] predict = res.getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testCalDistanceVecMatrix() {
		FastDistanceMatrixData matrixData = initMatrixData();
		FastDistanceVectorData vectorData = distance.prepareVectorData(Row.of(0, sparseVector1), 1);

		double[] predict = distance.calc(matrixData, vectorData).getData();
		double[] expect = new double[] {10.7, 16.7};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		predict = distance.calc(vectorData, matrixData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		vectorData = distance.prepareVectorData(Row.of(0, denseVector1), 1);

		predict = distance.calc(matrixData, vectorData).getData();
		expect = new double[] {0.0, 14.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		predict = distance.calc(vectorData, matrixData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testCalDistanceVecSparse() {
		FastDistanceVectorData vectorData = distance.prepareVectorData(Row.of(0, denseVector1), 1);
		FastDistanceSparseData sparseData = initSparseData();

		double[] predict = distance.calc(sparseData, vectorData).getData();
		double[] expect = new double[] {10.7, 10.7};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		predict = distance.calc(vectorData, sparseData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		vectorData = distance.prepareVectorData(Row.of(0, sparseVector1), 1);
		expect = new double[] {0.0, 1.0};
		predict = distance.calc(vectorData, sparseData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testCalDistanceSparseSparse() {
		FastDistanceSparseData sparseData = initSparseData();
		DenseMatrix res = distance.calc(sparseData, sparseData);
		double[] expect = new double[] {0.0, 1.0, 1.0, 1.0, 0.0, 0.0};
		double[] predict = res.getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testException1() {
		FastDistanceData sparseData = initSparseData();
		FastDistanceData denseData = initMatrixData();
		thrown.expect(RuntimeException.class);
		distance.calc(denseData, sparseData, new DenseMatrix());
		distance.calc(sparseData, denseData, new DenseMatrix());
	}

}