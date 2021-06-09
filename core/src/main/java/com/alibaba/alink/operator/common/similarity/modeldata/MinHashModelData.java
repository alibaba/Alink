package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.similarity.JaccardSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.MinHashSimilarity;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class MinHashModelData extends HashModelData {
	private static final long serialVersionUID = 4672277163671668648L;
	private final Map <Object, int[]> data;
	private final MinHashSimilarity similarity;
	private final boolean text;

	public MinHashModelData(Map <Integer, List <Object>> indexMap, Map <Object, int[]> data,
							MinHashSimilarity similarity, boolean text) {
		super(indexMap);
		this.data = data;
		this.similarity = similarity;
		this.text = text;
		comparator = Comparator.comparingDouble(o -> o.f0);
	}

	@Override
	protected Tuple2 <Object, int[]> getSampleAndHashValues(Object input) {
		int[] sorted = similarity.getSorted(text ? Sample.split((String) input) : input);
		int[] hashValue = similarity.getMinHash(sorted);
		int[] bucket = similarity.toBucket(hashValue);
		int[] sample = similarity instanceof JaccardSimilarity ? sorted : hashValue;
		return Tuple2.of(sample, bucket);
	}

	@Override
	protected Double computeHashDistance(Object inputSample, Object sample) {
		return similarity.crossSimilarity((int[])inputSample, data.get(sample));
	}
}
