package com.alibaba.alink.operator.common.distance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Here we define Jaccard distance = 1 - Jaccard calc.
 *
 * Given two vectors a and b, Jaccard distance = 1 - ||indices(a) intersect indices(b)|| / ||indices(a) union
 * indices(b)||,
 * here indices(a) means the set of indices of those values who are not zero in vector a. ||*|| means the size of the
 * set.
 */
public class JaccardDistance extends FastDistance {
	private static final long serialVersionUID = -8437702143860565621L;
	private static int LABEL_SIZE = 1;

	@Override
	public double calc(double[] array1, double[] array2) {
		if (array1.length > array2.length) {
			double[] tmp = array1;
			array1 = array2;
			array2 = tmp;
		}
		int intersect = 0, union = 0;
		for (int i = 0; i < array1.length; i++) {
			if (array1[i] != 0 && array2[i] != 0) {
				intersect++;
				union++;
			} else if (array1[i] != 0 || array2[i] != 0) {
				union++;
			}
		}
		for (int i = array1.length; i < array2.length; i++) {
			if (array2[i] != 0) {
				union++;
			}
		}
		return 1 - (double) intersect / union;
	}

	@Override
	public double calc(Vector vec1, Vector vec2) {
		if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			int[] indices1 = ((SparseVector) vec1).getIndices();
			int[] indices2 = ((SparseVector) vec2).getIndices();
			int intersect = intersect(indices1, indices2);
			int union = indices1.length + indices2.length - intersect;
			return 1 - (double) intersect / union;

		} else if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return calc(((DenseVector) vec1).getData(), ((DenseVector) vec2).getData());

		} else {
			int[] indices;
			double[] data;
			if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
				indices = ((SparseVector) vec2).getIndices();
				data = ((DenseVector) vec1).getData();
			} else {
				// TODO(xiafei.qiuxf): if both vec1 and vec2 are null, it may fall to this branch. any pre-check?
				indices = ((SparseVector) vec1).getIndices();
				data = ((DenseVector) vec2).getData();
			}
			int intersect = 0, union = 0;
			int index1 = 0;
			for (int i = 0; i < data.length; i++) {
				if (index1 < indices.length && indices[index1] == i) {
					if (data[i] != 0) {
						intersect++;
					}
					index1++;
					union++;
				} else {
					if (data[i] != 0) {
						union++;
					}
				}
			}
			return 1 - (double) intersect / union;
		}
	}

	private static int intersect(int[] indices1, int[] indices2) {
		int index1 = 0;
		int index2 = 0;
		int intersect = 0;
		while (index1 < indices1.length && index2 < indices2.length) {
			if (indices1[index1] == indices2[index2]) {
				intersect++;
				index1++;
				index2++;
			} else if (indices1[index1] < indices2[index2]) {
				index1++;
			} else {
				index2++;
			}
		}
		return intersect;
	}

	private static int intersect(double[] indices1, double[] indices2) {
		int index1 = 0;
		int index2 = 0;
		int intersect = 0;
		while (index1 < indices1.length && index2 < indices2.length) {
			if (indices1[index1] == indices2[index2]) {
				intersect++;
				index1++;
				index2++;
			} else if (indices1[index1] < indices2[index2]) {
				index1++;
			} else {
				index2++;
			}
		}
		return intersect;
	}

	@Override
	List <FastDistanceData> prepareDenseMatrixData(Tuple2 <Vector, Row> tuple,
												   Iterator <Tuple2 <Vector, Row>> iterator,
												   int vectorSize) {
		List <FastDistanceData> list = new ArrayList <>();
		list.add(prepareVectorData(tuple));
		while (iterator.hasNext()) {
			tuple = iterator.next();
			list.add(prepareVectorData(tuple));
		}
		return list;
	}

	@Override
	public void updateLabel(FastDistanceData data) {
		if (data instanceof FastDistanceVectorData) {
			FastDistanceVectorData vectorData = (FastDistanceVectorData) data;
			if (vectorData.vector instanceof DenseVector) {
				double[] dataArray = ((DenseVector) vectorData.vector).getData();
				List <Integer> list = new ArrayList <>();
				for (int i = 0; i < dataArray.length; i++) {
					if (dataArray[i] == 0) {
						list.add(i);
					}
				}
				double[] zeroArray = new double[list.size()];
				for (int i = 0; i < list.size(); i++) {
					zeroArray[i] = list.get(i);
				}
				vectorData.label = new DenseVector(zeroArray);
			}
		} else if (data instanceof FastDistanceSparseData) {
			FastDistanceSparseData sparseData = (FastDistanceSparseData) data;
			if (sparseData.label == null || sparseData.label.numCols() != sparseData.vectorNum
				|| sparseData.label.numRows() != LABEL_SIZE) {
				sparseData.label = new DenseMatrix(LABEL_SIZE, sparseData.vectorNum);
			}
			double[] vectorLabel = sparseData.label.getData();
			int[][] indices = sparseData.getIndices();
			for (int[] indice : indices) {
				if (null != indice) {
					for (int j = 0; j < indice.length; j++) {
						vectorLabel[indice[j]]++;
					}
				}
			}
		}
	}

	@Override
	double calc(FastDistanceVectorData left, FastDistanceVectorData right) {
		if (left.vector instanceof DenseVector && right.vector instanceof DenseVector) {
			int zeroIntersect = intersect(left.label.getData(), right.label.getData());
			int nonZeroIntersect = left.vector.size() - left.label.size() - right.label.size() + zeroIntersect;
			int union = 2 * left.vector.size() - left.label.size() - right.label.size() - nonZeroIntersect;
			return 1 - (double) nonZeroIntersect / union;
		}
		return calc(left.vector, right.vector);
	}

	@Override
	void calc(FastDistanceVectorData vector, FastDistanceMatrixData matrix, double[] res) {
		throw new RuntimeException("Jaccard Distance not support matrix calculation yet!");
	}

	@Override
	void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res) {
		throw new RuntimeException("Jaccard Distance not support matrix calculation yet!");
	}

	@Override
	void calc(FastDistanceVectorData left, FastDistanceSparseData right, double[] res) {
		Arrays.fill(res, 0.0);
		int[][] rightIndices = right.getIndices();
		if (left.vector instanceof DenseVector) {
			double[] zeroArray = left.label.getData();
			for (double index : zeroArray) {
				int i = (int) index;
				if (null != rightIndices[i]) {
					for (int j = 0; j < rightIndices[i].length; j++) {
						res[rightIndices[i][j]]++;
					}
				}
			}
			int vectorSize = left.vector.size();
			int leftSize = zeroArray.length;
			double[] sparseDataNonZeroNumber = right.getLabel().getData();
			for (int i = 0; i < res.length; i++) {
				res[i] = 1 - (sparseDataNonZeroNumber[i] - res[i]) / (vectorSize + res[i] - leftSize);
			}
		} else {
			int[] nonZeroArray = ((SparseVector) left.getVector()).getIndices();
			for (int i : nonZeroArray) {
				if (null != rightIndices[i]) {
					for (int j = 0; j < rightIndices[i].length; j++) {
						res[rightIndices[i][j]]++;
					}
				}
			}
			int leftSize = nonZeroArray.length;
			double[] sparseDataNonZeroNumber = right.getLabel().getData();
			for (int i = 0; i < res.length; i++) {
				res[i] = 1 - res[i] / (leftSize + sparseDataNonZeroNumber[i] - res[i]);
			}
		}

	}

	@Override
	void calc(FastDistanceSparseData left, FastDistanceSparseData right, double[] res) {
		Arrays.fill(res, 0.0);
		int[][] leftIndices = left.getIndices();
		int[][] rightIndices = right.getIndices();

		Preconditions.checkArgument(leftIndices.length == rightIndices.length, "VectorSize not equal!");
		for (int i = 0; i < leftIndices.length; i++) {
			int[] leftIndicesList = leftIndices[i];
			int[] rightIndicesList = rightIndices[i];
			if (null != leftIndicesList) {
				for (int aLeftIndicesList : leftIndicesList) {
					int startIndex = aLeftIndicesList * right.vectorNum;
					if (null != rightIndicesList) {
						for (int aRightIndicesList : rightIndicesList) {
							res[startIndex + aRightIndicesList]++;
						}
					}
				}
			}
		}
		int leftCnt = 0;
		int rightCnt = 0;
		int numRow = left.vectorNum;
		double[] leftNonZeroNumber = left.label.getData();
		double[] rightNonZeroNumber = right.label.getData();
		for (int i = 0; i < res.length; i++) {
			if (rightCnt == numRow) {
				rightCnt = 0;
				leftCnt++;
			}
			res[i] = 1 - res[i] / (rightNonZeroNumber[rightCnt++] + leftNonZeroNumber[leftCnt] - res[i]);
		}
	}

}
