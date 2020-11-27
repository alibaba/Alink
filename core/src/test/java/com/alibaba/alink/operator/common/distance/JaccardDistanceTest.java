package com.alibaba.alink.operator.common.distance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for JaccardDistance.
 */
public class JaccardDistanceTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private JaccardDistance distance = new JaccardDistance();
	private DenseVector denseVector1 = new DenseVector(new double[] {1, 0, 4, 0, 3});
	private DenseVector denseVector2 = new DenseVector(new double[] {0, 6, 1, 0, 4});
	private SparseVector sparseVector1 = new SparseVector(5, new int[] {1, 3}, new double[] {0.1, 0.4});
	private SparseVector sparseVector2 = new SparseVector(5, new int[] {2, 3}, new double[] {0.4, 0.1});

	@Test
	public void testContinuousDistance() {
		Assert.assertEquals(distance.calc(denseVector1, denseVector2), 0.5, 0.01);
		Assert.assertEquals(distance.calc(denseVector1.getData(), denseVector2.getData()), 0.5, 0.01);
		Assert.assertEquals(distance.calc(new double[] {1, 0, 4}, new double[] {1, 0}), 0.5, 0.01);
		Assert.assertEquals(distance.calc(denseVector1, sparseVector1), 1.0, 0.01);
		Assert.assertEquals(distance.calc(sparseVector1, sparseVector2), 0.66, 0.01);
		Assert.assertEquals(distance.calc(sparseVector1, denseVector2), 0.75, 0.01);
	}

	private List <FastDistanceData> initMatrixData() {
		List <Row> dense = new ArrayList <>();
		dense.add(Row.of(0, denseVector1));
		dense.add(Row.of(1, denseVector2));
		return distance.prepareMatrixData(dense, 1);
	}

	private FastDistanceSparseData initSparseData() {
		List <Row> sparse = new ArrayList <>();
		sparse.add(Row.of(0, sparseVector1));
		sparse.add(Row.of(1, sparseVector2));
		return (FastDistanceSparseData) distance.prepareMatrixData(sparse, 1).get(0);
	}

	@Test
	public void testUpdateLabel() {
		DenseVector label = distance.prepareVectorData(Tuple2.of(denseVector1, null)).getLabel();
		Assert.assertEquals(label.getData().length, 2);
		double[][] data = new double[][] {{1.0, 3.0}, {0.0, 3.0}};
		for (int i = 0; i < data.length; i++) {
			Assert.assertEquals(label.getData()[i], data[0][i], 0.01);
		}

		List <FastDistanceData> matrixData = initMatrixData();

		Assert.assertEquals(matrixData.size(), 2);
		for (int i = 0; i < data.length; i++) {
			double[] labelData = ((FastDistanceVectorData) matrixData.get(i)).label.getData();
			for (int j = 0; j < data[i].length; j++) {
				Assert.assertEquals(labelData[j], data[i][j], 0.01);
			}
		}

		FastDistanceSparseData sparseData = initSparseData();
		Assert.assertEquals(sparseData.label.numCols(), 2);
		Assert.assertEquals(sparseData.label.numRows(), 1);
		double[] labelData = sparseData.label.getData();
		double[] expect = new double[] {2.0, 2.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], labelData[i], 0.01);
		}
	}

	@Test
	public void testCalDistanceVecVec() {
		DenseMatrix denseResult = distance.calc(distance.prepareVectorData(Tuple2.of(denseVector1, null)),
			(FastDistanceData) distance.prepareVectorData(Tuple2.of(denseVector2, null)));
		Assert.assertEquals(denseResult.get(0, 0), 0.5, 0.01);

		DenseMatrix sparseResult = distance.calc(distance.prepareVectorData(Tuple2.of(sparseVector1, null)),
			(FastDistanceData) distance.prepareVectorData(Tuple2.of(sparseVector2, null)));
		Assert.assertEquals(sparseResult.get(0, 0), 0.66, 0.01);
	}

	@Test
	public void testCalDistanceVecSparse() {
		FastDistanceVectorData vectorData = distance.prepareVectorData(Row.of(0, denseVector1), 1);
		FastDistanceSparseData sparseData = initSparseData();

		double[] predict = distance.calc(sparseData, vectorData).getData();
		double[] expect = new double[] {1.0, 0.75};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		predict = distance.calc(vectorData, sparseData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}

		vectorData = distance.prepareVectorData(Row.of(0, sparseVector1), 1);
		expect = new double[] {0.0, 0.66};
		predict = distance.calc(vectorData, sparseData).getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testCalDistanceSparseSparse() {
		FastDistanceSparseData sparseData = initSparseData();
		DenseMatrix res = distance.calc(sparseData, sparseData);
		double[] expect = new double[] {0.0, 0.66, 0.66, 0.0};
		double[] predict = res.getData();
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], predict[i], 0.01);
		}
	}

	@Test
	public void testException1() {
		List <Row> dense = new ArrayList <>();
		dense.add(Row.of(0, denseVector1));
		dense.add(Row.of(1, denseVector2));
		FastDistanceMatrixData data = (FastDistanceMatrixData) new EuclideanDistance().prepareMatrixData(dense, 1).get(
			0);
		thrown.expect(RuntimeException.class);
		distance.calc(distance.prepareVectorData(Tuple2.of(denseVector1, null)), data);
		distance.calc(data, data);
	}

}