package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.io.Serializable;

/**
 * Base class for calculating continuous distance.
 *
 * It supports calculating distances between two double arrays, two vectors or two DenseVectors.
 */
public interface ContinuousDistance extends Serializable {

	/**
	 * Calculate the distance between two double arrays, the length of the array must be equal.
	 * @param array1 array1
	 * @param array2 array2
	 * @return the distance
	 */
	double calc(double[] array1, double[] array2);

	/**
	 * Calculate the distance between two vectors.
	 * @param vec1 vector1
	 * @param vec2 vector2
	 * @return the distance
	 */
	double calc(Vector vec1, Vector vec2);

	/**
	 * Calculate the distance between two dense vectors
	 * @param vec1 densevector1
	 * @param vec2 densevector2
	 * @return the distance
	 */
	default double calc(DenseVector vec1, DenseVector vec2) {
		return calc(vec1.getData(), vec2.getData());
	}
}
