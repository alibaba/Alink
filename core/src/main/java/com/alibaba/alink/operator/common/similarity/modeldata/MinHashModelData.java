package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.similarity.JaccardSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.MinHashSimilarity;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class MinHashModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 4672277163671668648L;
	private Map <Object, int[]> data;
	private Map <Integer, List <Object>> indexMap;
	private MinHashSimilarity similarity;
	private boolean text;

	private Iterator <Object> iterator;
	private HashSet <Object> set;
	private int[] hashValue;

	public MinHashModelData(Map <Integer, List <Object>> indexMap, Map <Object, int[]> data,
							MinHashSimilarity similarity, boolean text) {
		this.indexMap = indexMap;
		this.data = data;
		this.similarity = similarity;
		this.text = text;
		queue = new PriorityQueue <>(Comparator.comparingDouble(o -> o.f0));
	}

	@Override
	void iterabor(Object selectedCol) {
		queue.clear();
		int[] sorted = similarity.getSorted(text ? Sample.split((String) selectedCol) : selectedCol);
		int[] hashValue = similarity.getMinHash(sorted);
		int[] bucket = similarity.toBucket(hashValue);

		set = new HashSet <>();
		for (int hash : bucket) {
			List <Object> list = indexMap.get(hash);
			if (null != list) {
				set.addAll(indexMap.get(hash));
			}
		}

		this.hashValue = similarity instanceof JaccardSimilarity ? sorted : hashValue;
		iterator = set.iterator();
	}

	@Override
	boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	void next(Tuple2 <Double, Object> newValue) {
		newValue.f1 = iterator.next();
		newValue.f0 = similarity.crossSimilarity(hashValue, data.get(newValue.f1));
	}
}
