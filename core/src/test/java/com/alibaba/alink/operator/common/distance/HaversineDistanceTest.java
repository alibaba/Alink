package com.alibaba.alink.operator.common.distance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.operator.common.distance.HaversineDistance.haverSine;

/**
 * Unit test for HaversineDistance.
 */

public class HaversineDistanceTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private HaversineDistance distance = new HaversineDistance();
	private DenseVector denseVector1 = new DenseVector(new double[] {40, 20});
	private DenseVector denseVector2 = new DenseVector(new double[] {10, 60});
	private SparseVector sparseVector1 = new SparseVector(5, new int[] {1, 3}, new double[] {0.1, 0.4});
	private SparseVector sparseVector2 = new SparseVector(5, new int[] {2, 3}, new double[] {0.4, 0.1});

	@Test
	public void testHaverSin() {
		Assert.assertEquals(0.25, haverSine(Math.PI / 3), 0.001);
	}

	@Test
	public void testDegreeToRadian() {
		Assert.assertEquals(Math.PI / 2, HaversineDistance.degreeToRadian(90), 0.001);
	}

	@Test
	public void testContinuousDistance() {
		Assert.assertEquals(distance.calc(denseVector1, denseVector2), 5160.251, 0.01);
		Assert.assertEquals(distance.calc(denseVector1.getData(), denseVector2.getData()), 5160.251, 0.01);

		thrown.expect(IllegalStateException.class);
		distance.calc(DenseVector.rand(3), DenseVector.rand(3));
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
		sparse.add(Row.of(2, sparseVector2));
		return (FastDistanceSparseData) distance.prepareMatrixData(sparse, 1).get(0);
	}

	@Test
	public void testUpdateLabel() {
		DenseVector label = distance.prepareVectorData(Tuple2.of(denseVector1, null)).getLabel();
		Assert.assertEquals(label.size(), 3);
		Assert.assertEquals(label.get(0), 0.642, 0.001);
		Assert.assertEquals(label.get(1), 0.719, 0.001);
		Assert.assertEquals(label.get(2), 0.262, 0.001);

		FastDistanceMatrixData matrixData = initMatrixData();

		Assert.assertEquals(matrixData.label.numCols(), 2);
		Assert.assertEquals(matrixData.label.numRows(), 3);
		double[] expect = new double[] {0.642, 0.719, 0.262, 0.173, 0.492, 0.852};
		double[] predict = matrixData.getLabel().getData();

		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.001);
		}

		thrown.expect(RuntimeException.class);
		FastDistanceSparseData sparseData = initSparseData();
	}

	@Test
	public void testCalDistanceVecVec() {
		DenseMatrix denseResult = distance.calc(distance.prepareVectorData(Tuple2.of(denseVector1, null)),
			(FastDistanceData) distance.prepareVectorData(Tuple2.of(denseVector2, null)));
		Assert.assertEquals(denseResult.get(0, 0), 5160.251, 0.01);
	}

	@Test
	public void testCalDistanceMatrixMatrix() {
		FastDistanceMatrixData matrixData = initMatrixData();
		DenseMatrix res = distance.calc(matrixData, matrixData);
		double[] expect = new double[] {0.0, 5160.251, 5160.251, 0.0};
		double[] predict = res.getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testCalDistanceVecMatrix() {
		FastDistanceMatrixData matrixData = initMatrixData();
		FastDistanceVectorData vectorData = distance.prepareVectorData(Row.of(0, denseVector1), 1);

		double[] predict = distance.calc(matrixData, vectorData).getData();
		double[] expect = new double[] {0.0, 5160.251};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		predict = distance.calc(vectorData, matrixData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testException1() {
		thrown.expect(IllegalStateException.class);
		distance.calc((Vector) DenseVector.rand(3), (Vector) DenseVector.rand(3));

		List <Row> sparse = new ArrayList <>();
		sparse.add(Row.of(0, sparseVector1));
		sparse.add(Row.of(1, sparseVector2));
		sparse.add(Row.of(1, sparseVector2));
		distance.prepareMatrixData(sparse, 1).get(0);
	}

	@Test
	public void testException2() {
		thrown.expect(RuntimeException.class);

		List <Row> sparse = new ArrayList <>();
		sparse.add(Row.of(0, sparseVector1));
		sparse.add(Row.of(1, sparseVector2));
		sparse.add(Row.of(1, sparseVector2));
		FastDistanceSparseData data = (FastDistanceSparseData) new EuclideanDistance().prepareMatrixData(sparse, 1)
			.get(
				0);
		distance.calc(data, data);
		distance.calc(distance.prepareVectorData(Row.of(0, denseVector1), 1), data);
	}
}