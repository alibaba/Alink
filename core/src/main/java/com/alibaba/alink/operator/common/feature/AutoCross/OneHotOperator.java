package com.alibaba.alink.operator.common.feature.AutoCross;

import com.alibaba.alink.common.linalg.SparseVector;

import java.util.Arrays;
import java.util.List;

public class OneHotOperator {
	List <int[]> crossFeatures;
	int[][] carry;
	int[] cunsumCrossFeatureSize;
	int[] cunsumFeatureSize;
	int dataSize;

	//original size is the value number of input data.
	OneHotOperator(int originSize, List <int[]> crossFeatures, int[] indexSize) {
		this.crossFeatures = crossFeatures;
		cunsumCrossFeatureSize = new int[crossFeatures.size() + 1];//存累积的feature size。第0个存origin，后面的存cross feature的。
		cunsumFeatureSize = new int[indexSize.length + 1];
		for (int i = 0; i < indexSize.length + 1; i++) {
			if (i == 0) {
				cunsumFeatureSize[i] = 0;
			} else {
				cunsumFeatureSize[i] = cunsumFeatureSize[i - 1] + indexSize[i - 1];
			}
		}
		Arrays.fill(cunsumCrossFeatureSize, 1);
		carry = new int[crossFeatures.size()][];//onehot进制
		for (int i = 0; i < crossFeatures.size(); i++) {
			int[] candidateFeature = crossFeatures.get(i);
			for (int j = 0; j < candidateFeature.length; j++) {
				if (j == 0) {
					carry[i] = new int[candidateFeature.length];
					carry[i][j] = 1;
				} else {
					carry[i][j] = carry[i][j - 1] * indexSize[candidateFeature[j - 1]];
				}
				cunsumCrossFeatureSize[i + 1] *= indexSize[candidateFeature[j]];
			}
		}
		cunsumCrossFeatureSize[0] = cunsumFeatureSize[cunsumFeatureSize.length - 1];
		for (int i = 1; i <= crossFeatures.size(); i++) {
			cunsumCrossFeatureSize[i] += cunsumCrossFeatureSize[i - 1];
		}
		dataSize = cunsumCrossFeatureSize[cunsumCrossFeatureSize.length - 1];
	}

	SparseVector oneHotData(SparseVector data) {
		int[] originIndices = data.getIndices();
		int indicesSize = originIndices.length + crossFeatures.size();
		int[] newIndices = new int[indicesSize];
		System.arraycopy(originIndices, 0, newIndices, 0, originIndices.length);
		for (int i = 0; i < crossFeatures.size(); i++) {
			newIndices[originIndices.length + i] = cunsumCrossFeatureSize[i]
				+ dot(carry[i], crossFeatures.get(i), originIndices, cunsumFeatureSize);
		}
		double[] newValues = new double[indicesSize];
		Arrays.fill(newValues, 1.0);
		return new SparseVector(dataSize, newIndices, newValues);
	}

	static int dot(int[] carry, int[] crossFeatures, int[] originIndices, int[] cunsumFeatureSize) {
		int res = 0;
		for (int i = 0; i < carry.length; i++) {
			res += carry[i] * (originIndices[crossFeatures[i]] - cunsumFeatureSize[crossFeatures[i]]);
		}
		return res;
	}
}