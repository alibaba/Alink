package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Calculator for FeatureBins, it
 */
public class FeatureBinsCalculator implements Serializable {
    private static String POSITIVE_INF = "+inf";
    private static String NEGATIVE_INF = "-inf";
    private static String LEFT_OPEN = "(";
    private static String LEFT_CLOSE = "[";
    private static String RIGHT_OPEN = ")";
    private static String RIGHT_CLOSE = "]";
    private static String JOIN_DELIMITER = ",";

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
    public Bins bin;

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
     * FeatureType of the FeatureBorder, used for calculation.
     */
    private BinTypes.ColType colType;

    /**
     * iv of the FeatureBorder, only used for the web.
     */
    private Double iv;

    /**
     * BinCount of the FeatureBorder, only used for the web.
     */
    private Integer binCount;

    /**
     * It's only used for calculation the statistics. It's ignored when serialized.
     */
    private Long total;

    /**
     * It's only used for calculation the statistics. It's ignored when serialized.
     */
    private Long positiveTotal;

    /**
     * Used for discrete Feature.
     */
    public static FeatureBinsCalculator createDiscreteCalculator(BinDivideType binDivideType,
                                                                 String featureName,
                                                                 TypeInformation<?> type,
                                                                 Bins bin){
        FeatureBinsCalculator calculator = new FeatureBinsCalculator();
        calculator.binDivideType = binDivideType;
        calculator.featureName = featureName;
        calculator.featureType = FeatureBinsUtil.getTypeString(type);
        calculator.colType = FeatureBinsUtil.featureTypeToColType(calculator.featureType, binDivideType);
        calculator.bin = (null == bin ? new Bins() : bin);
        return calculator;
    }

    /**
     * Used for Continuous Feature.
     */
    public static FeatureBinsCalculator createNumericCalculator(BinDivideType binDivideType,
                                                                String featureName,
                                                                TypeInformation<?> type,
                                                                Number[] splitsArray,
                                                                Boolean isLeftOpen){
        FeatureBinsCalculator calculator = new FeatureBinsCalculator();
        calculator.binDivideType = binDivideType;
        calculator.featureName = featureName;
        calculator.featureType = FeatureBinsUtil.getTypeString(type);
        calculator.colType = FeatureBinsUtil.featureTypeToColType(calculator.featureType, binDivideType);
        calculator.isLeftOpen = Preconditions.checkNotNull(isLeftOpen);
        Preconditions.checkNotNull(splitsArray);
        Tuple2<Bins, Number[]> t = FeatureBinsUtil.createNumericBin(splitsArray);
        calculator.bin = t.f0;
        calculator.splitsArray = t.f1;
        return calculator;
    }

    public Boolean getLeftOpen() {
        return isLeftOpen;
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
        return FeatureBinsUtil.keepGivenDecimal(iv, 3);
    }

    public boolean isNumeric(){
        return colType.isNumeric;
    }

    public BinTypes.ColType getColType() {
        return colType;
    }

    public String getFeatureName() {
        return featureName;
    }

    public String getFeatureType() {
        return featureType;
    }

    public Number[] getSplitsArray() {
        return splitsArray;
    }

    public BinDivideType getBinDivideType() {
        return binDivideType;
    }

    public Bins getBin() {
        return bin;
    }

    /**
     * When clearTotal, it means all the data must be re-calculated, so the positiveTotal must be cleared as well.
     */
    private void clearTotal() {
        if (null != bin.elseBin) {
            bin.elseBin.total = null;
        }
        if (null != bin.nullBin) {
            bin.nullBin.total = null;
        }
        for (Bins.BaseBin normBin : bin.normBins) {
            normBin.total = null;
        }
        this.total = null;
        clearPositiveTotal();
    }

    private void clearPositiveTotal() {
        if (null != bin.elseBin) {
            bin.elseBin.positive = null;
        }
        if (null != bin.nullBin) {
            bin.nullBin.positive = null;
        }
        for (Bins.BaseBin normBin : bin.normBins) {
            normBin.positive = null;
        }
        this.positiveTotal = null;
    }

    public void setTotal(Map<Long, Long> indexTotalMap) {
        clearTotal();
        checkBeforeSetTotal(indexTotalMap);
        if(!colType.isNumeric) {
            bin.elseBin.total = indexTotalMap.getOrDefault(bin.elseBin.index, 0L);
        }
        bin.nullBin.total = indexTotalMap.getOrDefault(bin.nullBin.index, 0L);
        for (Bins.BaseBin normBin : bin.normBins) {
            normBin.total = indexTotalMap.getOrDefault(normBin.index, 0L);
        }
    }

    public void setPositiveTotal(Map<Long, Long> indexTotalMap) {
        clearPositiveTotal();
        checkBeforeSetTotal(indexTotalMap);
        if(!colType.isNumeric) {
            bin.elseBin.positive = indexTotalMap.getOrDefault(bin.elseBin.index, 0L);
        }
        bin.nullBin.positive = indexTotalMap.getOrDefault(bin.nullBin.index, 0L);
        for (Bins.BaseBin normBin : bin.normBins) {
            normBin.positive = indexTotalMap.getOrDefault(normBin.index, 0L);
        }
    }

    private void checkBeforeSetTotal(Map<Long, Long> indexTotalMap) {
        Preconditions.checkState(indexTotalMap.size() > 0, "Total is not set!");
        indexTotalMap.keySet().forEach(
            index -> Preconditions.checkArgument(index >= 0 && index < FeatureBinsUtil.getBinEncodeVectorSize(this),
                "Predict Index greater than binCounts, featureName: " + featureName + "; index: " + index
                    + "; BinCounts: " + FeatureBinsUtil.getBinEncodeVectorSize(this)));
        this.checkIndex();
    }

    /**
     * SplitsArray must be strictly increasing and have no duplicate values.
     */
    void checkSplitsArray() {
        Preconditions.checkNotNull(isLeftOpen, "LeftOpen is undefined");
        if (null == splitsArray) {
            splitsArray = new Number[0];
        }
        if (this.bin == null || this.bin.normBins == null) {
            Tuple2<Bins, Number[]> t = FeatureBinsUtil.createNumericBin(splitsArray);
            this.bin = t.f0;
            this.splitsArray = t.f1;
        } else {
            for (int i = 1; i < splitsArray.length; i++) {
                Preconditions.checkArgument(FeatureBinsUtil.compareNumbers(splitsArray[i], splitsArray[i - 1]) > 0,
                    "SplitsArray must be strictly increasing!");
            }
            Preconditions.checkArgument(splitsArray.length + 1 == this.bin.normBins.size());
        }
    }

    /**
     * NormBins could be empty but could not be null, and each BaseBin must contain at least one value.
     */
    void checkDiscreteNormBins() {
        Preconditions.checkNotNull(bin.normBins, "NormBins could not be NULL!");
        for (Bins.BaseBin baseBin : bin.normBins) {
            Preconditions.checkNotNull(baseBin.values, "Border Array is NULL!");
            Preconditions.checkState(baseBin.values.size() > 0,
                "DiscreteIntervalValuesError, size:%s", baseBin.values.size());
        }
    }

    private void checkIndex() {
        HashSet<Long> list = new HashSet<>();
        Preconditions.checkNotNull(bin, "Bin is empty!");
        Preconditions.checkNotNull(bin.normBins, "NormBinArray could not be NULL!");
        //extract norm bins
        for (Bins.BaseBin normBin : bin.normBins) {
            Preconditions.checkArgument(normBin.index >= 0 && normBin.index < bin.normBins.size(),
                "Index must be continuous, current index:", normBin.index + "; Norm bin size:" + bin.normBins.size());
            list.add(normBin.index);
        }
        Preconditions.checkArgument(list.size() == bin.normBins.size());
        if (null != bin.nullBin) {
            bin.nullBin.index = FeatureBinsUtil.nullIndex(bin.normBins.size());
        } else {
            bin.nullBin = new Bins.BaseBin(FeatureBinsUtil.nullIndex(bin.normBins.size()));
        }

        if (!colType.isNumeric) {
            if (null != bin.elseBin) {
                bin.elseBin.index = FeatureBinsUtil.elseIndex(bin.normBins.size());
            } else {
                bin.elseBin = new Bins.BaseBin(FeatureBinsUtil.elseIndex(bin.normBins.size()));
            }
        }
    }

    /**
     * For the normBins, nullBin, elseBin, if not empty, calculate the inner statistics.
     * Before calculating, fisrt must make sure the index is continuous and clear the old data.
     */
    public void calcStatistics() {
        this.checkIndex();
        this.binCount = bin.normBins.size();
        this.total = null;
        this.positiveTotal = null;
        for (Bins.BaseBin normBin : bin.normBins) {
            calcTotal(normBin);
        }
        calcTotal(bin.nullBin);
        calcTotal(bin.elseBin);
        if (null != total) {
            for (Bins.BaseBin bin : bin.normBins) {
                calcBinStatistics(bin);
            }
            calcBinStatistics(bin.nullBin);
            calcBinStatistics(bin.elseBin);
        }
    }

    private void calcTotal(Bins.BaseBin bin) {
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

    /**
     * Calculator the inner statistics(woe/iv/positiveRate/negativeRate...) of the given bin.
     * @param bin
     */
    public void calcBinStatistics(Bins.BaseBin bin) {
        if (null == bin) {
            return;
        }
        bin.setStatisticsData(total, colType, positiveTotal);
        Double binIv = bin.getIV();
        if (null != binIv) {
            iv = (null == iv ? binIv : iv + binIv);
        }
    }

    public void tryToUpdateBins(Bins bins){
        if(null != bins){
            this.bin = bins;
        }
    }

    /**
     * For numeric FeatureBins, sometimes we need to transform the split points to interval.
     * It's often used for output or web.
     */
    public void splitsArrayToInterval() {
        if(!isNumeric()){
            return;
        }
        int size = splitsArray.length;
        Preconditions.checkState(size + 1 == bin.normBins.size(),
            "Norm Bin size not equal to the size of splitsArray length + 1");
        String[] intervals = FeatureBinsUtil.cutsArrayToInterval(splitsArray, getLeftOpen());
        List<Bins.BaseBin> list = bin.normBins;
        Preconditions.checkState(intervals.length == list.size(), "Interval length not equal to the size of normBins!");
        for(int i = 0; i < list.size(); i++){
            list.get(i).values = Collections.singletonList(intervals[i]);
        }
    }

}
