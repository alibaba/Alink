package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.common.similarity.Sample;

import java.util.Collections;
import java.util.HashSet;

public class JaccardSimilarity extends MinHashSimilarity {

	private static final long serialVersionUID = -3809874353849488951L;

	public JaccardSimilarity(Long seed, int k, int b) {
		super(seed, k, b);
	}

	/**
	 * JaccardSim = |A ∩ B| / |A ∪ B| = |A ∩ B| / (|A| + |B| - |A ∩ B|)
	 */
	@Override
	public double crossSimilarity(int[] left, int[] right) {
		int i = 0;
		int j = 0;
		double sameCnt = 0;
		int leftSize = left.length;
		int rightSize = right.length;
		if (leftSize == 0 && rightSize == 0) {
			return 0.0;
		}
		while (i < leftSize && j < rightSize) {
			if (left[i] == right[j]) {
				sameCnt += 1.0;
				i += 1;
				j += 1;
			} else if (left[i] > right[j]) {
				j += 1;
			} else {
				i += 1;
			}
		}
		return sameCnt / ((double) leftSize + (double) rightSize - sameCnt);
	}

	@Override
	public <M> void updateLabel(Sample sample, M str) {
		int[] sorted = getSorted(str);
		int[] hashValue = getMinHash(sorted);
		int[] buckets = toBucket(hashValue);
		sample.setStr(null);
		sample.setLabel(new int[][] {sorted, buckets});
	}

	public static <T> double similarity(T left, T right){
		int cross = 0;
		if(left instanceof String){
			HashSet<Character> set = new HashSet <>();
			String leftStr = (String)left;
			String rightStr = (String)right;
			for(int i = 0; i < leftStr.length(); i++){
				set.add(leftStr.charAt(i));
			}
			for(int i = 0; i < rightStr.length(); i++){
				if(set.contains(rightStr.charAt(i))){
					cross++;
				}
			}
			return 1.0 * cross / (leftStr.length() + rightStr.length() - cross);
		}else if(left instanceof String[]){
			HashSet<String> set = new HashSet <>();
			String[] leftStr = (String[])left;
			String[] rightStr = (String[])right;
			Collections.addAll(set, leftStr);
			Collections.addAll(set, rightStr);
			return 1.0 * (leftStr.length + rightStr.length - set.size()) / set.size();
		}else{
			throw new AkUnsupportedOperationException("Only support String and String array yet!");
		}
	}
}
