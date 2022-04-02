package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * Extract by directly parsing the JSON string.
 */
class DefaultLabelProbMapExtractor implements LabelProbMapExtractor {

	private static final long serialVersionUID = 7388010330390496141L;

	@Override
	public Map <String, Double> extract(String json) {
		try {
			return JsonConverter.fromJson(json,
				new TypeReference <HashMap <String, Double>>() {}.getType());
		} catch (Exception e) {
			throw new RuntimeException(
				String.format("Failed to deserialize prediction detail: %s.", json));
		}
	}
}
