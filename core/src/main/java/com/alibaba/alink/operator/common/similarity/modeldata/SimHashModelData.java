package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.distance.SimHashHammingDistance;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.dataConverter.SimHashModelDataConverter;
import com.alibaba.alink.operator.common.similarity.similarity.SimHashHammingSimilarity;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SimHashModelData extends HashModelData {
	private static final long serialVersionUID = 1274421101444113676L;
	private final SimHashHammingSimilarity similarity;
	private final SimHashHammingDistance distance;
	private final StringTextApproxNearestNeighborTrainParams.Metric metric;

	private final Map <Object, BigInteger> data;
	private final boolean text;

	public SimHashModelData(Map <Integer, List <Object>> indexMap,
							Map <Object, BigInteger> data,
							StringTextApproxNearestNeighborTrainParams.Metric metric,
							boolean text) {
		super(indexMap);
		this.data = data;
		this.metric = metric;
		this.text = text;
		similarity = new SimHashHammingSimilarity();
		distance = (SimHashHammingDistance) similarity.getDistance();
		if (metric.equals(StringTextApproxNearestNeighborTrainParams.Metric.SIMHASH_HAMMING_SIM)) {
			comparator = Comparator.comparingDouble(v -> v.f0);
		} else {
			comparator = Comparator.comparingDouble(v -> -v.f0);
		}
	}

	@Override
	protected Tuple2 <Object, int[]> getSampleAndHashValues(Object input) {
		BigInteger bi = text ? distance.simHash(Sample.split((String) input)) : distance.simHash(input);
		int[] hashes = SimHashModelDataConverter.splitBigInteger(bi);
		return Tuple2.of(bi, hashes);
	}

	@Override
	protected Double computeHashDistance(Object inputSample, Object sample) {
		return metric.equals(
			StringTextApproxNearestNeighborTrainParams.Metric.SIMHASH_HAMMING_SIM) ?
			similarity.similarity((BigInteger) inputSample, data.get(sample)) :
			distance.hammingDistance((BigInteger) inputSample, data.get(sample));
	}
}
