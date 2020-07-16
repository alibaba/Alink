package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Saved split points for Quantile and EqualWith discretizer. For each selected continous feature, the split points is
 * included in a ContinousFeatureInterval.
 */
public class ContinuousRanges implements Serializable {
    private static final ObjectMapper JSON_INSTANCE = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    /**
     * Each feature contains a splitArray.
     */
    public String featureName;

    /**
     * FeatureType of the FeatureBorder, used for the web.
     */
    public String featureType;

    /**
     * SplitsArray for the continuous FeatureBorder. It's null for discrete FeatureBorder.
     */
    public Number[] splitsArray;

    /**
     * isLeftOpen for the continuous FeatureBorder. It's null for discrete FeatureBorder.
     */
    private Boolean isLeftOpen;

    /**
     * Type is float or splitsArray is float;
     */
    private Boolean isFloat;

    /**
     * Usered for deserialize.
     */
    ContinuousRanges() {
    }

    /**
     * User for Continous Feature.
     * @param featureName The featureName for this feature.
     * @param type  The TypeInformation for the given feature.
     * @param splitsArray   The splits points of the given feature.
     * @param isLeftOpen    Is the splits leftOpen or not, for example, splitsArray is [-1,2], if leftOpen, the inverval
     *                      is (-inf, -1],(-1, 2],(2,+inf).
     */
    public ContinuousRanges(String featureName, TypeInformation<?> type, Number[] splitsArray,
                            Boolean isLeftOpen) {
        this.featureName = featureName;
        this.featureType = FeatureBinsUtil.getTypeString(type);
        Preconditions.checkNotNull(isLeftOpen);
        if (splitsArray == null) {
            splitsArray = new Number[0];
        }
        this.splitsArray = splitsArray;
        this.isLeftOpen = isLeftOpen;
    }

    public Boolean getLeftOpen() {
        return isLeftOpen;
    }

    public boolean isFloat() {
        if(null == this.isFloat){
            TypeInformation<?> typeInformation = FeatureBinsUtil.getFlinkType(this.featureType);
            this.isFloat = typeInformation.equals(Types.DOUBLE) || typeInformation.equals(Types.FLOAT);
            for(Number number : splitsArray){
                this.isFloat |= (number instanceof Double || number instanceof Float);
            }
        }
        return this.isFloat;
    }

    public int getIntervalNum() {
        return splitsArray.length + 1;
    }

    public static ContinuousRanges deSerialize(String str) {
        if (null == str) {
            return null;
        }
        try {
            /**
             * For compatible, ignores the invalid keys in the older model.
             */
            ContinuousRanges[] featureIntervals = JSON_INSTANCE.readValue(str,
                JSON_INSTANCE.getTypeFactory().constructType(ContinuousRanges[].class));
            return featureIntervals[0];
        } catch (IOException e) {
            throw new IllegalArgumentException("Deserialize json to object fail. json: " + str, e);
        }
    }

    public static String serialize(ContinuousRanges featureInterval) {
        try {
            return JSON_INSTANCE.writeValueAsString(new ContinuousRanges[] {featureInterval});
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Serialize object to json fail.", e);
        }
    }
}
