package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.distance.FastCategoricalDistance;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.similarity.Similarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class StringModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 7378206633997295502L;
	private final Sample[] dictSamples;
	private final FastCategoricalDistance similarity;
	private final boolean text;

	public StringModelData(Sample[] dictSamples, FastCategoricalDistance similarity, boolean text) {
		this.dictSamples = dictSamples;
		this.similarity = similarity;
		this.text = text;

		if (similarity instanceof Similarity) {
			comparator = Comparator.comparingDouble(o -> o.f0);
		} else {
			comparator = Comparator.comparingDouble(o -> -o.f0);
		}
	}

	@Override
	protected Integer getLength() {
		return dictSamples.length;
	}

	@Override
	protected Object prepareSample(Object input) {
		return similarity.prepareSample((String) input, text);
	}

	@Override
	protected ArrayList <Tuple2 <Double, Object>> computeDistiance(Object input, Integer index, Integer topN,
																   Tuple2 <Double, Object> radius) {
		Tuple2 <Double, Object> value = Tuple2.of(
			similarity.calc(dictSamples[index], (Sample)input, text),
			dictSamples[index].getRow().getField(0)
		);
		if (null == radius || radius.f0 == null || this.getQueueComparator().compare(radius, value) <= 0) {
			return new ArrayList <>(Arrays.asList(value));
		}
		return null;
	}
}
