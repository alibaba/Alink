package com.alibaba.alink.operator.common.distance;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.similarity.Sample;

/**
 * FastCompareCategoricalDistance is used to accelerate the speed of calculating the distance by pre-calculating
 * some extra info of the string.
 */
public interface FastCategoricalDistance<T> {

	default Sample prepareSample(String str, Row row, boolean text) {
		Sample sample = new Sample(str, row);
		updateLabel(sample, text ? Sample.split(str) : str);
		return sample;
	}

	default Sample prepareSample(String str, boolean text) {
		return prepareSample(str, null, text);
	}

	default Sample prepareSample(boolean text, Row row, int strIdx, int... keepIdxs) {
		String str = (String) row.getField(strIdx);
		row = TableUtil.getRow(row, keepIdxs);
		return prepareSample(str, row, text);
	}

	double calc(Sample <T> left, Sample <T> right, boolean text);

	default <M> void updateLabel(Sample sample, M str) {}
}
