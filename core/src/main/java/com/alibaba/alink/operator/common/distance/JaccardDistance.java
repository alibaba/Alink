package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.util.ArrayList;
import java.util.List;

/**
 * Here we define Jaccard distance = 1 - Jaccard similarity.
 *
 * Given two vectors a and b, Jaccard distance = 1 - ||indices(a) intersect indices(b)|| / ||indices(a) union indices(b)||,
 * here indices(a) means the set of indices of those values who are not zero in vector a. ||*|| means the size of the set.
 */
public class JaccardDistance extends FastDistance {
	@Override
	public double calc(double[] array1, double[] array2) {
		if(array1.length > array2.length){
			double[] tmp = array1;
			array1 = array2;
			array2 = tmp;
		}
		int intersect = 0, union = 0;
		for(int i = 0; i < array1.length; i++){
			if(array1[i] != 0 && array2[i] != 0){
				intersect++;
				union++;
			}else if(array1[i] != 0 || array2[i] != 0){
				union++;
			}
		}
		for(int i = array1.length; i < array2.length; i++){
			if(array2[i] != 0){
				union++;
			}
		}
		return 1 - (double) intersect / union;
	}

	@Override
	public double calc(Vector vec1, Vector vec2) {
		if(vec1 instanceof SparseVector && vec2 instanceof SparseVector){
			int[] indices1 = ((SparseVector)vec1).getIndices();
			int[] indices2 = ((SparseVector)vec2).getIndices();
			int intersect = intersect(indices1, indices2);
			int union = indices1.length + indices2.length - intersect;
			return 1 - (double) intersect / union;

		}else if(vec1 instanceof DenseVector && vec2 instanceof DenseVector){
			return calc(((DenseVector)vec1).getData(), ((DenseVector)vec2).getData());

		}else{
			int[] indices;
			double[] data;
			if(vec1 instanceof DenseVector && vec2 instanceof SparseVector){
				indices = ((SparseVector)vec2).getIndices();
				data = ((DenseVector)vec1).getData();
			}else{
				indices = ((SparseVector)vec1).getIndices();
				data = ((DenseVector)vec2).getData();
			}
			int intersect = 0, union = 0;
			int index1 = 0;
			for(int i = 0; i < data.length; i++){
				if(index1 < indices.length && indices[index1] == i){
					if(data[i] != 0){
						intersect++;
					}
					index1++;union++;
				}else{
					if(data[i] != 0){
						union++;
					}
				}
			}
			return 1 - (double) intersect / union;
		}
	}

	private static int intersect(int[] indices1, int[] indices2){
		int index1 = 0;
		int index2 = 0;
		int intersect = 0;
		while(index1 < indices1.length && index2 < indices2.length){
			if(indices1[index1] == indices2[index2]){
				intersect++;
				index1++; index2++;
			}else if(indices1[index1] < indices2[index2]){
				index1++;
			}else{
				index2++;
			}
		}
		return intersect;
	}

	private static int intersect(double[] indices1, double[] indices2){
		int index1 = 0;
		int index2 = 0;
		int intersect = 0;
		while(index1 < indices1.length && index2 < indices2.length){
			if(indices1[index1] == indices2[index2]){
				intersect++;
				index1++; index2++;
			}else if(indices1[index1] < indices2[index2]){
				index1++;
			}else{
				index2++;
			}
		}
		return intersect;
	}

	@Override
	void calc(FastDistanceVectorData vector, FastDistanceMatrixData matrix, double[] res){
		throw new RuntimeException("Jaccard Distance not support matrix calculation yet!");
	}

	@Override
	void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res){
		throw new RuntimeException("Jaccard Distance not support matrix calculation yet!");
	}

	@Override
	public void updateLabel(FastDistanceData data){
		if(data instanceof FastDistanceVectorData){
			FastDistanceVectorData vectorData = (FastDistanceVectorData)data;
			if(vectorData.vector instanceof DenseVector){
				double[] dataArray = ((DenseVector)vectorData.vector).getData();
				List<Integer> list = new ArrayList<>();
				for(int i = 0; i < dataArray.length; i++){
					if(dataArray[i] != 0){
						list.add(i);
					}
				}
				double[] zeroArray = new double[list.size()];
				for(int i = 0; i < list.size(); i++){
					zeroArray[i] = list.get(i);
				}
				vectorData.label = new DenseVector(zeroArray);
			}
		}
	}

	@Override
	double calc(FastDistanceVectorData left, FastDistanceVectorData right){
		if(left.vector instanceof DenseVector && right.vector instanceof DenseVector){
			int nonZeroIntersect = intersect(left.label.getData(), right.label.getData());
			int zeroIntersect = left.vector.size() - left.label.size() - right.label.size() + nonZeroIntersect;
			int union = 2 * left.vector.size() - zeroIntersect;
			return 1 - (double) zeroIntersect / union;
		}
		return calc(left.vector, right.vector);
	}

}
