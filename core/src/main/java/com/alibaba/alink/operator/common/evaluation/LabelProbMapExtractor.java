package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;

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
			AkPreconditions.checkState(v <= 1.0 && v >= 0,
				new AkIllegalDataException(String.format("Probability in %s not in range [0, 1]!", json))));

		final double PROB_SUM_EPS = 0.01;
		AkPreconditions.checkState(
			Math.abs(probs.stream().mapToDouble(Double::doubleValue).sum() - 1.0) < PROB_SUM_EPS,
			new AkIllegalDataException(String.format("Probability sum in %s not equal to 1.0!", json)));
		return labelProbMap;
	}
}
