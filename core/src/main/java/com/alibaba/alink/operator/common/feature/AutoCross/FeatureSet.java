package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FeatureSet implements Serializable {
	private static final long serialVersionUID = 3050538250196533180L;
	public int numRawFeatures;//the feature number of all the input feature, including numerical and categorical.
	public String[] numericalCols;
	public String vecColName;//the name of all the input features.
	public List <int[]> crossFeatureSet;//save all the crossed features.
	public List <Double> scores;
	public int[] indexSize;
	public boolean hasDiscrete;
	@JsonIgnore
	public double[] fixedCoefs = new double[0];

	/**
	 * Create initial set composing of all non-crossed features.
	 */
	public FeatureSet(int[] featureSize) {
		//featureSize is each value of onehot feature, so numRawFeatures is the sum of numRawFeatures.
		this.numRawFeatures = featureSize.length;
		crossFeatureSet = new ArrayList <>();
		scores = new ArrayList <>();
	}


	public void updateFixedCoefs(double[] fixedCoefs) {
		this.fixedCoefs = fixedCoefs;
	}

	public List <int[]> generateCandidateCrossFeatures() {
		int n = crossFeatureSet.size() + numRawFeatures;
		List <int[]> existingFeatures = new ArrayList <>();//saves the index of existing features.
		for (int i = 0; i < numRawFeatures; i++) {
			existingFeatures.add(new int[] {i});
		}
		existingFeatures.addAll(crossFeatureSet);
		Set <IntArrayComparator> pairwiseCrossFea = new HashSet <>(n * (n - 1) / 2);
		for (int i = 0; i < n; i++) {
			for (int j = i + 1; j < n; j++) {
				int[] crossIJ = ArrayUtils.addAll(existingFeatures.get(i),
					existingFeatures.get(j));//cross two features.
				//remove duplicate features. If the crossed features are "1 1 2", then it will be transformed to "1 2"
				crossIJ = removeDuplicate(crossIJ);
				Arrays.sort(crossIJ);
				if (!this.contains(crossIJ)) {
					pairwiseCrossFea.add(new IntArrayComparator(crossIJ));
				}
			}
		}
		List <int[]> res = new ArrayList <>(pairwiseCrossFea.size());
		for (IntArrayComparator i : pairwiseCrossFea) {
			res.add(i.data);
		}
		return res;
	}

	//judge whether crossFeatureSet contains crossFea.
	private boolean contains(int[] crossFea) {
		for (int[] target : crossFeatureSet) {
			if (crossFea.length != target.length) {
				continue;
			}
			boolean same = true;
			for (int j = 0; j < target.length; j++) {
				if (target[j] != crossFea[j]) {
					same = false;
					break;
				}
			}
			if (same) {
				return true;
			}
		}
		return false;
	}

	private static int[] removeDuplicate(int[] input) {
		Set <Integer> set = new HashSet <>();
		for (int anInput : input) {
			set.add(anInput);
		}
		int[] ret = new int[set.size()];
		int cnt = 0;
		for (Integer v : set) {
			ret[cnt++] = v;
		}
		return ret;
	}

	public void addOneCrossFeature(int[] crossFea, double score) {
		this.crossFeatureSet.add(crossFea);
		this.scores.add(score);
	}

	public double[] getFixedCoefs() {
		return fixedCoefs;
	}

	@Override
	public String toString() {
		assert crossFeatureSet.size() == scores.size();
		return JsonConverter.toJson(this);
	}


	private class IntArrayComparator {
		int[] data;

		IntArrayComparator() {}

		IntArrayComparator(int[] data) {
			this.data = data;
		}


		@Override
		public int hashCode() {
			StringBuilder sbd = new StringBuilder();
			for (int datum : data) {
				sbd.append(datum).append(",");
			}
			return sbd.toString().hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null) return false;
			if (this.getClass() != o.getClass()) return false;
			return o.hashCode() == this.hashCode();
		}
	}
}
