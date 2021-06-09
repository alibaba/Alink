package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.similarity.SimilarityUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class HashModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 911628973324779241L;

	private final Map <Integer, List <Object>> indexMap;

	protected HashModelData(Map <Integer, List <Object>> map) {
		this.indexMap = map;
	}

	@Override
	protected void search(Object input, Integer topN,
						  Tuple2 <Double, Object> radius,
						  PriorityQueue <Tuple2 <Double, Object>> priorityQueue) {
		Tuple2 <Object, int[]> t = getSampleAndHashValues(input);
		Object inputSample = t.f0;
		int[] hashValues = t.f1;
		Tuple2 <Double, Object> head = null;
		HashSet <Object> sampleSet = getBucketSamples(hashValues);
		for (Object sample : sampleSet) {
			Tuple2 <Double, Object> tuple = computeHashDistance(inputSample, sample, radius);
			if (null == tuple) {continue;}
			if (null == topN) {
				priorityQueue.add(Tuple2.of(tuple.f0, tuple.f1));
			} else {
				head = SimilarityUtil.updateQueue(priorityQueue, topN, tuple, head);
			}
		}
	}

	protected Tuple2 <Object, int[]> getSampleAndHashValues(Object input) {
		return null;
	}

	protected Tuple2 <Double, Object> computeHashDistance(Object input, Object sample,
														  Tuple2 <Double, Object> radius) {
		Tuple2 <Double, Object> tuple = Tuple2.of(computeHashDistance(input, sample), sample);
		if (null == radius || radius.f0 == null || this.getQueueComparator().compare(radius, tuple) <= 0) {
			return tuple;
		}
		return null;
	}

	protected Double computeHashDistance(Object inputSample, Object sample) {
		return null;
	}

	protected HashSet <Object> getBucketSamples(int[] hashValues) {
		HashSet <Object> set = new HashSet <>();
		for (int hash : hashValues) {
			List <Object> list = indexMap.get(hash);
			if (null != list) {
				set.addAll(indexMap.get(hash));
			}
		}
		return set;
	}

}
