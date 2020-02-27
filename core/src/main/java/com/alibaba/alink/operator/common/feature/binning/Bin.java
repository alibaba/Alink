package com.alibaba.alink.operator.common.feature.binning;

import com.google.common.base.Joiner;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bin for Featureborder.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Bin implements Serializable {
    /**
     * Join delimiter for discrete values.
     */
    private static String JOIN_DELIMITER = ",";

    /**
     * Normal bin.
     */
    @JsonProperty("NORM")
    public List<BaseBin> normBins;

    /**
     * Null bin.
     */
    @JsonProperty("NULL")
    public BaseBin nullBin;

    /**
     * Else bin.
     */
    @JsonProperty("ELSE")
    public BaseBin elseBin;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class BaseBin implements Serializable {
        /**
         * Values can be converted to and from borders. It's only used before serialize.
         */
        List<String> values;

        /**
         * Borders can be converted to and from values. It's used in calculation.
         */
        @JsonIgnore
        ArrayList<BinTypes.SingleBorder> borders;

        /**
         * The woe of this bin, it could be null.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Double woe;

        /**
         * The total of this bin, it could be null.
         */
        Long total;

        /**
         * The positve total of this bin, it could be null.
         */
        Long positive;

        /**
         * The index of this bin.
         */
        Long index;

        /**
         * The negative total of this bin, total = positive + negative, it could be null.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Long negative;

        /**
         * The totalRate of this bin in the whole featureborder, totalRate = binTotal / featureborderTotal.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Double totalRate;

        /**
         * The positiveRate of this bin in the whole featureborder, positiveRate = binPositive / featureborderPositive.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Double positiveRate;

        /**
         * The negativeRate of this bin in the whole featureborder, negativeRate = binNegative / featureborderNegative.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Double negativeRate;

        /**
         * The percentage of positive samples in this bin, positivePercentage = binPositive / binTotal.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Double positivePercentage;

        /**
         * Iv value of this bin, it could be null.
         */
        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private Double iv;

        public Double getIV() {
            return BinningUtil.keepGivenDecimal(iv, 3);
        }

        public Double getPositivePercentage() {
            return BinningUtil.keepGivenDecimal(positivePercentage, 4);
        }

        public List<String> getValues() {
            return values;
        }

        public String getValueStr(BinTypes.ColType colType) {
            return colType.isNumeric ? values.get(0) : Joiner.on(JOIN_DELIMITER).join(values);
        }

        public ArrayList<BinTypes.SingleBorder> getBorders() {
            return borders;
        }

        public Long getIndex() {
            return index;
        }

        public Long getTotal() {
            return total;
        }

        public Long getPositive() {
            return positive;
        }

        public Double getWoe() {
            return BinningUtil.keepGivenDecimal(woe, 3);
        }

        public Long getNegative() {
            return negative;
        }

        public Double getPositiveRate() {
            return BinningUtil.keepGivenDecimal(positiveRate, 4);
        }

        public Double getNegativeRate() {
            return BinningUtil.keepGivenDecimal(negativeRate, 4);
        }


        public Double getTotalRate() {
            return BinningUtil.keepGivenDecimal(totalRate, 4);
        }

        BaseBin() {}

        public BaseBin(Long index, BinTypes.SingleBorder... singleBorders) {
            this.index = index;
            if (singleBorders.length > 0) {
                borders = new ArrayList<>();
                borders.addAll(Arrays.asList(singleBorders));
            }
        }

        public BaseBin(Long index, String[] strs){
            this.index = index;
            if(strs.length > 0){
                values = new ArrayList<>();
                values.addAll(Arrays.asList(strs));
            }
        }

        void setStatisticsData(Long total, BinTypes.ColType colType, Long positiveTotal) {
            this.total = null == this.total ? 0L : this.total;
            Preconditions.checkNotNull(total, "Total is NULL!");
            Preconditions.checkNotNull(colType, "FeatureType is NULL!");

            totalRate = 1.0 * this.total / total;
            if (null != positiveTotal) {
                positive = null == positive ? 0L : positive;
                negative = this.total - positive;
                Preconditions.checkArgument(negative >= 0,
                    "Total must be greater or equal than Positive! Total:" + this.total
                        + "; Positive: " + positive);
                if (this.total > 0) {
                    positivePercentage = 1.0 * positive / this.total;
                }
                if (positiveTotal > 0) {
                    positiveRate = 1.0 * positive / positiveTotal;
                }
                if (total - positiveTotal > 0) {
                    negativeRate = 1.0 * negative / (total - positiveTotal);
                    if (negative > 0 && positive > 0) {
                        woe = Math.log(positiveRate / negativeRate);
                        iv = (positiveRate - negativeRate) * woe;
                    }
                }
            }

        }

        void bordersToValues(BinTypes.ColType colType) {
            if (null != borders) {
                values = BinningUtil.singleBorderArrayToStr(borders, colType);
            }
        }

        public BinTypes.SingleBorder left() {
            return borders.get(0);
        }

        public BinTypes.SingleBorder right() {
            return borders.get(1);
        }
    }
}
