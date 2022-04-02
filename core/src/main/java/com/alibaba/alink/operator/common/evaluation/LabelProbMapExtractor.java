package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

@FunctionalInterface
public interface LabelProbMapExtractor extends Serializable {
	/**
	 * Extract the probability map from prediction detail string in JSON format.
	 *
	 * @param json prediction detail string
	 * @return the probability map
	 */
	Map <String, Double> extract(String json);

	/**
	 * Extract the probability map from prediction detail string in JSON format, and check if the map forms a valid
	 * probability distribution.
	 *
	 * @param json prediction detail string
	 * @return the probability map
	 */
	default Map <String, Double> extractAndCheck(String json) {
		Map <String, Double> labelProbMap = extract(json);

		Collection <Double> probs = labelProbMap.values();
		probs.forEach(v ->
			Preconditions.checkArgument(v <= 1.0 && v >= 0,
				String.format("Probability in %s not in range [0, 1]!", json)));

		final double PROB_SUM_EPS = 0.01;
		Preconditions.checkArgument(
			Math.abs(probs.stream().mapToDouble(Double::doubleValue).sum() - 1.0) < PROB_SUM_EPS,
			String.format("Probability sum in %s not equal to 1.0!", json));
		return labelProbMap;
	}
}
