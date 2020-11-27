package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.distance.FastCategoricalDistance;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.similarity.Similarity;

import java.util.Comparator;
import java.util.PriorityQueue;

public class StringModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 7378206633997295502L;
	private Sample[] dictSamples;
	private FastCategoricalDistance similarity;
	private boolean text;

	private Sample sample;
	private int start;

	public StringModelData(Sample[] dictSamples, FastCategoricalDistance similarity, boolean text) {
		this.dictSamples = dictSamples;
		this.similarity = similarity;
		this.text = text;

		if (similarity instanceof Similarity) {
			queue = new PriorityQueue <>(Comparator.comparingDouble(o -> o.f0));
		} else {
			queue = new PriorityQueue <>(Comparator.comparingDouble(o -> -o.f0));
		}
	}

	@Override
	void iterabor(Object selectedCol) {
		queue.clear();
		sample = similarity.prepareSample((String) selectedCol, text);
		start = 0;
	}

	@Override
	boolean hasNext() {
		return start < dictSamples.length;
	}

	@Override
	void next(Tuple2 <Double, Object> newValue) {
		newValue.f1 = dictSamples[start].getRow().getField(0);
		newValue.f0 = similarity.calc(dictSamples[start], sample, text);
		start++;
	}
}
