package com.alibaba.alink.operator.common.similarity.modeldata;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * When multiple hash functions are used, if different hash functions produce same values, they should not be treated as
 * collisions. Therefore, (hash_value, hash_function_id) should be the key when finding collisions, which is different
 * from {@link HashModelData}. To make the key (hash_value, hash_function_id) compact, we use (hash_value *
 * num_hash_functions + hash_function_id) as the key.
 */
public class LocalHashModelData extends HashModelData {
	private static final long serialVersionUID = 911628973324779241L;
	private final int numHashTables;
	private final Map <Long, Collection <Object>> indexMap;

	/*
	 In https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/p253-datar.pdf, only 3 * topN candidates are
	 retrieved. We let users set this value.
	*/
	private int maxNumCandidates = Integer.MAX_VALUE;

	protected LocalHashModelData(int numHashTables, Map <Long, Collection <Object>> map) {
		super(null);
		this.numHashTables = numHashTables;
		this.indexMap = map;
	}

	public void setMaxNumCandidates(int maxNumCandidates) {
		this.maxNumCandidates = maxNumCandidates;
	}

	protected HashSet <Object> getBucketSamples(int[] hashValues) {
		HashSet <Object> s = new HashSet <>();
		for (int i = 0; i < hashValues.length; i++) {
			int hashValue = hashValues[i];
			long hashValueWithIndex = ((long) hashValue) * numHashTables + i;
			Collection <Object> c = indexMap.get(hashValueWithIndex);
			if (null == c) {
				continue;
			}
			if (c.size() <= maxNumCandidates - s.size()) {
				s.addAll(c);
			} else {
				for (Object e : c) {
					if (s.size() >= maxNumCandidates) {
						break;
					}
					s.add(e);
				}
			}
			if (s.size() >= maxNumCandidates) {
				break;
			}
		}
		return s;
	}
}
