package com.alibaba.alink.operator.common.feature.binning;

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Featureborder for binning, discrete Featureborder and continuous Featureborder.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FeatureBorder implements Serializable {
    private static final ObjectMapper JSON_INSTANCE = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    /**
     * BinDivideType for this FeatureBorder.
     */
    public BinTypes.BinDivideType binDivideType;

    /**
     * Each feature is a FeatureBorder.
     */
    public String featureName;

    /**
     * Save the statistics data and index of the bins of this featureBorder.
     */
    public Bin bin;

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
     * FeatureType of the FeatureBorder, used for calculation.
     */
    @JsonIgnore
    public BinTypes.ColType colType;

    /**
     * iv of the FeatureBorder, only used for the web.
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Double iv;

    /**
     * BinCount of the FeatureBorder, only used for the web.
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Integer binCount;

    /**
     * It's only used for calculation the statistics. It's ignored when serialized.
     */
    @JsonIgnore
    private Long total;

    /**
     * It's only used for calculation the statistics. It's ignored when serialized.
     */
    @JsonIgnore
    private Long positiveTotal;

    FeatureBorder() {}

    public FeatureBorder(BinTypes.BinDivideType binDivideType, String featureName, TypeInformation type, Bin bin) {
        this(binDivideType, featureName, type, bin, null);
    }

    public FeatureBorder(BinTypes.BinDivideType binDivideType, String featureName, TypeInformation type, Bin bin,
                         Boolean isLeftOpen) {
        this.binDivideType = binDivideType;
        this.featureName = featureName;
        this.featureType = getTypeString(type);
        this.colType = featureTypeToColType(this.featureType, this.binDivideType);
        this.bin = bin;
        if (colType.isNumeric) {
            Preconditions.checkNotNull(isLeftOpen);
        }
        this.isLeftOpen = isLeftOpen;
    }

    public Long getTotal() {
        return total;
    }

    public Long getPositiveTotal() {
        return positiveTotal;
    }

    public int getBinCount() {
        Preconditions.checkNotNull(binCount, "BinCount is not set!");
        return binCount;
    }

    public Double getIv() {
        return BinningUtil.keepGivenDecimal(iv, 3);
    }

    public void clearTotalAndPositiveTotal() {
        if (null != bin.elseBin) {
            bin.elseBin.total = null;
            bin.elseBin.positive = null;
        }
        if (null != bin.nullBin) {
            bin.nullBin.total = null;
            bin.nullBin.positive = null;
        }
        if (null != bin.normBins) {
            for (Bin.BaseBin normBin : bin.normBins) {
                normBin.total = null;
                normBin.positive = null;
            }
        }
        this.total = null;
        this.positiveTotal = null;
    }

    public void setTotal(Map<Long, Long> indexTotalMap) {
        Preconditions.checkState(indexTotalMap.size() > 0, "Total is not set!");
        checkIndexMap(indexTotalMap.keySet());
        clearTotalAndPositiveTotal();
        //only discrete bins have ELSE bin
        if (!colType.isNumeric) {
            if (null == bin.elseBin) {
                bin.elseBin = new Bin.BaseBin();
            }
            bin.elseBin.index = BinningUtil.elseIndex(bin.normBins.size());
            bin.elseBin.total = indexTotalMap.getOrDefault(bin.elseBin.index, 0L);
        }
        if (null == bin.nullBin) {
            bin.nullBin = new Bin.BaseBin();
        }
        bin.nullBin.index = BinningUtil.nullIndex(bin.normBins.size());
        bin.nullBin.total = indexTotalMap.getOrDefault(bin.nullBin.index, 0L);
        if (null != bin.normBins) {
            for (Bin.BaseBin normBin : bin.normBins) {
                normBin.total = indexTotalMap.getOrDefault(normBin.index, 0L);
            }
        }
    }

    public void setPositiveTotal(Map<Long, Long> indexTotalMap) {
        Preconditions.checkState(indexTotalMap.size() > 0, "PositiveTotal is not set!");
        checkIndexMap(indexTotalMap.keySet());
        if (!colType.isNumeric) {
            if (null == bin.elseBin) {
                bin.elseBin = new Bin.BaseBin();
            }
            bin.elseBin.index = BinningUtil.elseIndex(bin.normBins.size());
            bin.elseBin.positive = indexTotalMap.getOrDefault(bin.elseBin.index, 0L);
        }
        if (null == bin.nullBin) {
            bin.nullBin = new Bin.BaseBin();
        }
        bin.nullBin.index = BinningUtil.nullIndex(bin.normBins.size());
        bin.nullBin.positive = indexTotalMap.getOrDefault(bin.nullBin.index, 0L);
        if (null != bin.normBins) {
            for (Bin.BaseBin normBin : bin.normBins) {
                normBin.positive = indexTotalMap.getOrDefault(normBin.index, 0L);
            }
        }
    }

    static Tuple2<Map<Long, Long>, Map<Long, Long>> getTotalAndPositiveTotal(FeatureBorder featureBorder) {
        Map<Long, Long> indexTotalMap = new HashMap<>();
        Map<Long, Long> indexPositiveTotalMap = new HashMap<>();
        if (null != featureBorder.bin.nullBin) {
            getTotalAndPositiveTotal(indexTotalMap, indexPositiveTotalMap,
                featureBorder.bin.nullBin, BinningUtil.nullIndex(featureBorder.bin.normBins.size()));
        }
        if (null != featureBorder.bin.elseBin) {
            getTotalAndPositiveTotal(indexTotalMap, indexPositiveTotalMap,
                featureBorder.bin.elseBin, BinningUtil.elseIndex(featureBorder.bin.normBins.size()));
        }
        if (null != featureBorder.bin.normBins) {
            for (Bin.BaseBin bin : featureBorder.bin.normBins) {
                getTotalAndPositiveTotal(indexTotalMap, indexPositiveTotalMap,
                    bin, bin.index);
            }
        }
        return Tuple2.of(indexTotalMap.size() > 0 ? indexTotalMap : null,
            indexPositiveTotalMap.size() > 0 ? indexPositiveTotalMap : null);
    }

    private static void getTotalAndPositiveTotal(Map<Long, Long> indexTotalMap,
                                                 Map<Long, Long> indexPositiveTotalMap,
                                                 Bin.BaseBin bin,
                                                 long index) {
        if (null != bin.total) {
            indexTotalMap.put(index, bin.total);
        }
        if (null != bin.positive) {
            indexPositiveTotalMap.put(index, bin.positive);
        }
    }

    private void checkIndexMap(Iterable<Long> indices) {
        indices.forEach(
            index -> Preconditions.checkArgument(index >= 0 && index < BinningUtil.getBinEncodeVectorSize(this),
                "Predict Index greater than binCounts, featureName: " + featureName + "; index: " + index
                    + "; BinCounts: " + BinningUtil.getBinEncodeVectorSize(this)));
    }

    public void prepareForSerialize() {
        checkNormBins(bin, colType);
        checkIndex();
        this.binCount = countBins(bin);
        Preconditions.checkState(binCount > 0, "FeatureBorder is empty!");
        calcTotal();
        if (colType.isNumeric) {
            Tuple2<Number[], Boolean> t = BinningUtil.borderToSplitsArray(bin.normBins, colType);
            splitsArray = t.f0;
            Preconditions.checkState(
                (splitsArray != null && splitsArray.length == 0) || isLeftOpen.equals(t.f1),
                "Inner error, checkout isLeftOpen parameter!"
            );
        }
        if (null != total) {
            calcBinStatistics();
        }
    }

    public void operationAfterDerialize() {
        this.colType = featureTypeToColType(featureType, binDivideType);
        if (null == this.bin) {
            this.bin = new Bin();
        }
        if (colType.isNumeric) {
            Preconditions.checkState(null == bin.elseBin, "Numeric bin not have else bin!");
            Tuple2<Map<Long, Long>, Map<Long, Long>> t = getTotalAndPositiveTotal(this);
            bin = BinningUtil.createNumericBin(splitsArray, isLeftOpen);
            if (null != t.f0) {
                setTotal(t.f0);
            }
            if (null != t.f1) {
                setPositiveTotal(t.f1);
            }
        }
        checkNormBins(bin, colType);
        checkIndex();
        this.binCount = countBins(bin);
        calcTotal();
        if (null != total) {
            calcBinStatistics();
        }
        Preconditions.checkState(binCount > 0, "FeatureBorder is empty!");
    }

    public static void borderToValue(Bin bin, BinTypes.ColType colType) {
        if (null != bin.nullBin) {
            bin.nullBin.bordersToValues(colType);
        }
        if (!colType.isNumeric && null != bin.elseBin) {
            bin.elseBin.bordersToValues(colType);
        }
        //extract norm bins
        if (null != bin.normBins) {
            for (Bin.BaseBin normBin : bin.normBins) {
                normBin.bordersToValues(colType);
            }
        }
    }

    private static void checkNormBins(Bin bin, BinTypes.ColType colType) {
        if (null == bin.normBins && colType.isNumeric) {
            Bin.BaseBin infBin = new Bin.BaseBin(0L,
                new BinTypes.SingleBorder(BinTypes.Symbol.GREATER, BinTypes.InfType.NEGATIVE_INF),
                new BinTypes.SingleBorder(BinTypes.Symbol.LESS, BinTypes.InfType.POSITIVE_INF));
            bin.normBins = Collections.singletonList(infBin);
        }
        if (null != bin.normBins) {
            BinningUtil.checkNormBinArray(bin.normBins, colType);
            BinningUtil.sortBaseBinArray(bin.normBins, colType);
            BinningUtil.checkNormBinArrayCrossAfterSort(bin.normBins, colType);
        }
    }

    private static BinTypes.ColType featureTypeToColType(String featureType, BinTypes.BinDivideType binDivideType) {
        return binDivideType.equals(BinTypes.BinDivideType.BUCKET) ? BinTypes.ColType.DOUBLE : BinTypes.ColType.valueOf(
            getFlinkType(featureType));
    }

    public static String getTypeString(TypeInformation<?> type) {
        String typeStr = TypeStringUtils.writeTypeInfo(type);
        if ("VARCHAR".equals(typeStr)) {
            typeStr = "STRING";
        }
        return typeStr;
    }

    public static TypeInformation<?> getFlinkType(String typeStr) {
        if ("STRING".equals(typeStr)) {
            typeStr = "VARCHAR";
        }
        return TypeStringUtils.readTypeInfo(typeStr);
    }

    private static int countBins(Bin bin) {
        return bin.normBins.size();
    }

    //ignore the index of null and else, the norm index must be continuous
    private void checkIndex() {
        HashSet<Long> list = new HashSet<>();
        Preconditions.checkNotNull(bin, "Bin is empty!");
        Preconditions.checkNotNull(bin.normBins, "There is no normal bin!");
        //extract norm bins
        for (Bin.BaseBin normBin : bin.normBins) {
            Preconditions.checkArgument(normBin.index >= 0 && normBin.index < bin.normBins.size(),
                "Index must be continuous, current index:", normBin.index + "; Norm bin size:" + bin.normBins.size());
            list.add(normBin.index);
        }
        Preconditions.checkArgument(list.size() == bin.normBins.size());
        if (null != bin.nullBin) {
            bin.nullBin.index = BinningUtil.nullIndex(bin.normBins.size());
        } else {
            bin.nullBin = new Bin.BaseBin(BinningUtil.nullIndex(bin.normBins.size()));
        }

        if (!colType.isNumeric) {
            if (null != bin.elseBin) {
                bin.elseBin.index = BinningUtil.elseIndex(bin.normBins.size());
            } else {
                bin.elseBin = new Bin.BaseBin(BinningUtil.elseIndex(bin.normBins.size()));
            }
        }
    }

    public void calcTotal() {
        this.total = null;
        this.positiveTotal = null;
        for (Bin.BaseBin normBin : bin.normBins) {
            calcTotal(normBin);
        }
        calcTotal(bin.nullBin);
        calcTotal(bin.elseBin);
    }

    private void calcTotal(Bin.BaseBin bin) {
        if (null == bin) {
            return;
        }
        if (null != bin.total) {
            total = (null == total ? bin.total : total + bin.total);
        }
        if (null != bin.positive) {
            positiveTotal = (null == positiveTotal ? bin.positive : positiveTotal + bin.positive);
        }
    }

    public void calcBinStatistics() {
        if (null != bin.normBins) {
            for (Bin.BaseBin bin : bin.normBins) {
                calcBinStatistics(bin);
            }
        }
        calcBinStatistics(bin.nullBin);
        calcBinStatistics(bin.elseBin);
    }

    public void calcBinStatistics(Bin.BaseBin bin) {
        if (null == bin) {
            return;
        }
        bin.setStatisticsData(total, colType, positiveTotal);
        Double binIv = bin.getIV();
        if (null != binIv) {
            iv = (null == iv ? binIv : iv + binIv);
        }
    }

    public static FeatureBorder[] deSerialize(String str) {
        if (null == str) {
            return null;
        }
        try {
            FeatureBorder[] featureBorders = JSON_INSTANCE.readValue(str,
                JSON_INSTANCE.getTypeFactory().constructType(FeatureBorder[].class));
            for (FeatureBorder featureBorder : featureBorders) {
                featureBorder.operationAfterDerialize();
            }
            return featureBorders;
        } catch (IOException e) {
            throw new IllegalArgumentException("Deserialize json to object fail. json: " + str, e);
        }
    }

    public static String serialize(FeatureBorder... featureBorders) {
        for (FeatureBorder featureBorder : featureBorders) {
            featureBorder.prepareForSerialize();
        }
        try {
            return JSON_INSTANCE.writeValueAsString(featureBorders);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Serialize object to json fail.", e);
        }
    }

    @Override
    public String toString() {
        this.prepareForSerialize();
        return JsonConverter.toJson(this);
    }
}
