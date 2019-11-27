package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.alink.operator.common.clustering.kmeans.LocalKmeansFunc.kmeans;

/**
 * Test for LocalKmeans.
 */
public class LocalKmeansFuncTest {
	@Test
	public void kmeansTest() {
		EuclideanDistance distance = new EuclideanDistance();
		long[] sampleWeights = new long[10];
		FastDistanceVectorData[] samples = new FastDistanceVectorData[sampleWeights.length];
		for (int i = 0; i < 10; i++) {
			sampleWeights[i] = i;
			samples[i] = distance.prepareVectorData(Tuple2.of(DenseVector.ones(2).scale(i), null));
		}
		FastDistanceMatrixData initCentroid = kmeans(2, sampleWeights, samples, distance, 2);
		DenseMatrix initCentroidData = initCentroid.getVectors();
		Assert.assertEquals(initCentroidData.numCols(), 2);
		Assert.assertEquals(new DenseVector(initCentroidData.getColumn(0)).normL2(), 10.842, 0.001);
		Assert.assertEquals(new DenseVector(initCentroidData.getColumn(1)).normL2(), 5.185, 0.001);
	}

	@Test
	public void kmeansSparseTest() {
		int len = 10;
		int k = 2;
		int size = 20;
		EuclideanDistance distance = new EuclideanDistance();
		long[] sampleWeights = new long[len];
		FastDistanceVectorData[] samples = new FastDistanceVectorData[len];
		for (int i = 0; i < 10; i++) {
			sampleWeights[i] = i;
			samples[i] = distance.prepareVectorData(Tuple2.of(new SparseVector(size, new int[]{i, i + 1}, new double[]{i, i}), null));
		}
		FastDistanceMatrixData initCentroid = kmeans(k, sampleWeights, samples, distance, size);
		DenseMatrix initCentroidData = initCentroid.getVectors();
		Assert.assertEquals(initCentroidData.numCols(), k);
		Assert.assertEquals(new DenseVector(initCentroidData.getColumn(0)).normL2(), 8.615, 0.001);
		Assert.assertEquals(new DenseVector(initCentroidData.getColumn(1)).normL2(), 4.128, 0.001);
	}

}