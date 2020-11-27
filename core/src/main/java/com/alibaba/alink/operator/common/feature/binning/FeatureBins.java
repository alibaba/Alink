package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * FeatureBins is used for serialize and deserialize, it's only connected to FeatureBinsCalculator.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FeatureBins implements Serializable {
	private static final ObjectMapper JSON_INSTANCE = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
		.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
		.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
	private static final long serialVersionUID = 3696539695552157161L;

	/**
	 * BinDivideType for this FeatureBorder.
	 */
	private BinDivideType binDivideType;

	/**
	 * Each feature is a FeatureBorder.
	 */
	private String featureName;

	/**
	 * Save the statistics data and index of the bins of this featureBorder.
	 */
	private Bins bin;

	/**
	 * FeatureType of the FeatureBorder, used for the web.
	 */
	private String featureType;

	/**
	 * SplitsArray for the continuous FeatureBorder. It's null for discrete FeatureBorder.
	 */
	private Number[] splitsArray;

	/**
	 * isLeftOpen for the continuous FeatureBorder. It's null for discrete FeatureBorder.
	 */
	private Boolean isLeftOpen;

	/**
	 * iv of the FeatureBorder, only used for the web.
	 */
	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private Double iv;

	/**
	 * BinCount of the FeatureBorder, only used for the web.
	 */
	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private int binCount;

	public BinDivideType getBinDivideType() {
		return binDivideType;
	}

	public String getFeatureName() {
		return featureName;
	}

	public Bins getBin() {
		return bin;
	}

	public String getFeatureType() {
		return featureType;
	}

	public Number[] getSplitsArray() {
		return splitsArray;
	}

	public Boolean getLeftOpen() {
		return isLeftOpen;
	}

	/**
	 * Used for discrete Feature.
	 */
	public static FeatureBins createDisreteFeatureBins(BinDivideType binDivideType,
													   String featureName,
													   String featureType,
													   Bins bin,
													   Double iv,
													   int binCount) {
		FeatureBins featureBins = new FeatureBins();
		featureBins.binDivideType = binDivideType;
		featureBins.featureName = featureName;
		featureBins.featureType = featureType;
		featureBins.bin = bin;
		featureBins.iv = iv;
		featureBins.binCount = binCount;
		return featureBins;
	}

	/**
	 * Used for discrete Feature.
	 */
	public static FeatureBins createNumericFeatureBins(BinDivideType binDivideType,
													   String featureName,
													   String featureType,
													   Bins bin,
													   Double iv,
													   int binCount,
													   Number[] splitsArray,
													   boolean isLeftOpen) {
		FeatureBins featureBins = new FeatureBins();
		featureBins.binDivideType = binDivideType;
		featureBins.featureName = featureName;
		featureBins.featureType = featureType;
		featureBins.bin = bin;
		featureBins.iv = iv;
		featureBins.binCount = binCount;
		featureBins.splitsArray = splitsArray;
		featureBins.isLeftOpen = isLeftOpen;
		return featureBins;
	}

	static FeatureBins[] deSerialize(String str) {
		if (null == str) {
			return null;
		}
		try {
			return JSON_INSTANCE.readValue(str,
				JSON_INSTANCE.getTypeFactory().constructType(FeatureBins[].class));
		} catch (IOException e) {
			throw new IllegalArgumentException("Deserialize json to object fail. json: " + str, e);
		}
	}

	static String serialize(FeatureBins... featureBins) {
		try {
			return JSON_INSTANCE.writeValueAsString(featureBins);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Serialize object to json fail.", e);
		}
	}
}
