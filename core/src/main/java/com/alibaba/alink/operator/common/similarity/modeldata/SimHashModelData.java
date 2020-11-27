package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.distance.SimHashHammingDistance;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.dataConverter.SimHashModelDataConverter;
import com.alibaba.alink.operator.common.similarity.similarity.SimHashHammingSimilarity;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class SimHashModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 1274421101444113676L;
	private SimHashHammingSimilarity similarity;
	private SimHashHammingDistance distance;
	private StringTextApproxNearestNeighborTrainParams.Metric metric;

	private Map <Object, BigInteger> data;
	private Map <Integer, List <Object>> indexMap;
	private boolean text;
	private BigInteger integer;
	private Iterator <Object> iterator;
	private HashSet <Object> set;

	public SimHashModelData(Map <Integer, List <Object>> indexMap,
							Map <Object, BigInteger> data,
							StringTextApproxNearestNeighborTrainParams.Metric metric,
							boolean text) {
		this.indexMap = indexMap;
		this.data = data;
		this.metric = metric;
		this.text = text;
		similarity = new SimHashHammingSimilarity();
		distance = (SimHashHammingDistance) similarity.getDistance();
		if (metric.equals(StringTextApproxNearestNeighborTrainParams.Metric.SIMHASH_HAMMING_SIM)) {
			queue = new PriorityQueue <>(Comparator.comparingDouble(v -> v.f0));
		} else {
			queue = new PriorityQueue <>(Comparator.comparingDouble(v -> -v.f0));
		}
	}

	@Override
	void iterabor(Object selectedCol) {
		queue.clear();
		integer = text ? distance.simHash(Sample.split((String) selectedCol)) : distance.simHash(selectedCol);
		int[] hashes = SimHashModelDataConverter.splitBigInteger(integer);
		set = new HashSet <>();
		for (int hash : hashes) {
			List <Object> list = indexMap.get(hash);
			if (null != list) {
				set.addAll(indexMap.get(hash));
			}
		}
		iterator = set.iterator();
	}

	@Override
	boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	void next(Tuple2 <Double, Object> newValue) {
		newValue.f1 = iterator.next();
		newValue.f0 = metric.equals(StringTextApproxNearestNeighborTrainParams.Metric.SIMHASH_HAMMING_SIM) ?
			similarity.similarity(integer, data.get(newValue.f1)) : distance.hammingDistance(integer,
			data.get(newValue.f1));
	}
}
