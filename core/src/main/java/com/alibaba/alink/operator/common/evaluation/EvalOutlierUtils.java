package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.outlier.OutlierDetector;
import com.google.gson.reflect.TypeToken;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.operator.common.outlier.OutlierDetector.IS_OUTLIER_KEY;
import static com.alibaba.alink.operator.common.outlier.OutlierDetector.OUTLIER_SCORE_KEY;

public class EvalOutlierUtils {
	public static final String OUTLIER_LABEL = "Outliers";
	public static final String INLIER_LABEL = "Normal";
	public static final Tuple2 <Map <Object, Integer>, Object[]> LABEL_INFO;

	static {
		Map <Object, Integer> labelIndexMap = new HashMap <>();
		labelIndexMap.put(OUTLIER_LABEL, 0);
		labelIndexMap.put(INLIER_LABEL, 1);
		LABEL_INFO = Tuple2.of(labelIndexMap, new Object[] {OUTLIER_LABEL, INLIER_LABEL});
	}

	/**
	 * Replace real labels to prediction labels: real labels in `outlierValueSet` are replaced with {@link
	 * #OUTLIER_LABEL}, while others with {@link #INLIER_LABEL}.
	 * <p>
	 * The input row should contain at least 1 fields, where the 1st field stores the real label. Other fields can be
	 * chosen to keep by setting `keepIndices`.
	 * <p>
	 * The 1st field of the output row is the replaced label, where other fields are kept fields if set.
	 */
	public static class ReplaceLabelMapFunction implements MapFunction <Row, Row> {

		private final Set <String> outlierValueSet;
		private final int[] keepIndices;

		public ReplaceLabelMapFunction(Set <String> outlierValueSet, int... keepIndices) {
			this.outlierValueSet = outlierValueSet;
			this.keepIndices = keepIndices;
		}

		@Override
		public Row map(Row value) {
			String label = outlierValueSet.contains(String.valueOf(value.getField(0)))
				? OUTLIER_LABEL
				: INLIER_LABEL;
			Row row = new Row(1 + keepIndices.length);
			row.setField(0, label);
			for (int i = 0; i < keepIndices.length; i += 1) {
				row.setField(1 + i, value.getField(keepIndices[i]));
			}
			return row;
		}
	}

	/**
	 * Extract the probability map from prediction details, where the details only contain the probability to be
	 * positive.
	 */
	public static class ProbMapExtractor implements LabelProbMapExtractor {
		@Override
		public Map <String, Double> extract(String json) {
			Map <String, Object> parsed;
			try {
				parsed = JsonConverter.fromJson(json,
					new TypeReference <HashMap <String, Object>>() {}.getType());
			} catch (Exception e) {
				throw new AkParseErrorException(
					String.format("Failed to deserialize prediction detail: %s.", json));
			}
			AkPreconditions.checkState(parsed.containsKey(OUTLIER_SCORE_KEY),
				String.format("Prediction detail %s doesn't contain key %s.", json, OUTLIER_SCORE_KEY));
			double p = Double.parseDouble(String.valueOf(parsed.get(OutlierDetector.OUTLIER_SCORE_KEY)));
			Map <String, Double> probMap = new HashMap <>();
			probMap.put(OUTLIER_LABEL, p);
			probMap.put(INLIER_LABEL, 1. - p);
			return probMap;
		}

		@Override
		public Map <String, Double> extractAndCheck(String json) {
			return extract(json);
		}
	}

	public static Tuple2 <Boolean, Double> extractPredictionScore(String detailJson) {
		Map <String, Object> m = JsonConverter.fromJson(detailJson,
			new TypeToken <Map <String, Object>>() {}.getType());
		AkPreconditions.checkState(m.containsKey(IS_OUTLIER_KEY),
			String.format("Prediction detail %s doesn't contain key %s.", detailJson, IS_OUTLIER_KEY));
		AkPreconditions.checkState(m.containsKey(OUTLIER_SCORE_KEY),
			String.format("Prediction detail %s doesn't contain key %s.", detailJson, OUTLIER_SCORE_KEY));
		boolean isOutlier = Boolean.parseBoolean(String.valueOf(m.get(IS_OUTLIER_KEY)));
		double score = Double.parseDouble(String.valueOf(m.get(OUTLIER_SCORE_KEY)));
		return Tuple2.of(isOutlier, score);
	}
}
