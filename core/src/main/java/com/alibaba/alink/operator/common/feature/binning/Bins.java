package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.google.common.base.Joiner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bin for Featureborder.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Bins implements Serializable {
	private static final long serialVersionUID = 9068326743869809322L;
	/**
	 * Join delimiter for discrete values.
	 */
	public static String JOIN_DELIMITER = ",";

	/**
	 * Normal bin, it could not be null.
	 */
	@JsonProperty("NORM")
	public List <BaseBin> normBins;

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

	public Bins() {
		this.normBins = new ArrayList <>();
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class BaseBin implements Serializable {
		private static final long serialVersionUID = 2550393461500614098L;
		/**
		 * Values can be converted to and from borders. It's only used before serialize.
		 */
		List <String> values;

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
		 * The positive total of this bin, it could be null.
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
			return FeatureBinsUtil.keepGivenDecimal(iv, 3);
		}

		public Double getPositivePercentage() {
			return FeatureBinsUtil.keepGivenDecimal(positivePercentage, 4);
		}

		public List <String> getValues() {
			return values;
		}

		public String getValueStr(BinTypes.ColType colType) {
			return colType.isNumeric ? values.get(0) : Joiner.on(JOIN_DELIMITER).join(values);
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
			return FeatureBinsUtil.keepGivenDecimal(woe, 3);
		}

		public Long getNegative() {
			return negative;
		}

		public Double getPositiveRate() {
			return FeatureBinsUtil.keepGivenDecimal(positiveRate, 4);
		}

		public Double getNegativeRate() {
			return FeatureBinsUtil.keepGivenDecimal(negativeRate, 4);
		}

		public Double getTotalRate() {
			return FeatureBinsUtil.keepGivenDecimal(totalRate, 4);
		}

		BaseBin() {}

		public BaseBin(Long index, String... strs) {
			this.index = index;
			if (strs.length > 0) {
				values = new ArrayList <>();
				values.addAll(Arrays.asList(strs));
			}
		}

		/**
		 * Adjusted Woe = ln((N(noevent + 0.5)/N(total_noevent))/(N(event + 0.5)/N(total_event))
		 *
		 * @param total
		 * @param colType
		 * @param positiveTotal
		 */
		void setStatisticsData(Long total, BinTypes.ColType colType, Long positiveTotal) {
			this.total = null == this.total ? 0L : this.total;
			AkPreconditions.checkNotNull(total, "Total is NULL!");
			AkPreconditions.checkNotNull(colType, "FeatureType is NULL!");

			totalRate = 1.0 * this.total / total;
			if (null != positiveTotal) {
				positive = null == positive ? 0L : positive;
				negative = this.total - positive;
				AkPreconditions.checkArgument(negative >= 0,
					"Total must be greater or equal than Positive! Total:" + this.total
						+ "; Positive: " + positive);
				if (this.total > 0) {
					positivePercentage = 1.0 * positive / this.total;
				}

				woe = FeatureBinsUtil.calcWoe(this.total, this.positive, positiveTotal, total - positiveTotal);
				if (woe.equals(Double.NaN)) {
					woe = null;
				} else {
					iv = (1.0 * positive / positiveTotal - 1.0 * negative / (total - positiveTotal)) * woe;
				}
			}

		}
	}

}
