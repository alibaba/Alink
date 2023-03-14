package com.alibaba.alink.operator.common.finance;

import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.similarity.SerializableComparator;

import java.io.Serializable;

public class VizData implements Serializable {
	private static final long serialVersionUID = 6180829409440580211L;
	public String featureName;
	public Long index;
	public String value;

	VizData() {}

	VizData(String featureName) {
		this(featureName, null, null);
	}

	VizData(String featureName, Long index, String value) {
		this.featureName = featureName;
		this.index = index;
		this.value = value;
	}

	public static class PSIVizData extends VizData {
		private static final long serialVersionUID = 6939286125661156752L;
		public Double testPercentage;
		public Double basePercentage;
		public Double testSubBase;
		public Double lnTestDivBase;
		public Double psi;

		PSIVizData() {}

		public PSIVizData(String featureName) {
			super(featureName);
		}

		public PSIVizData(String featureName, Long index, String value) {
			super(featureName, index, value);
		}

		public void calcPSI() {
			testSubBase = testPercentage - basePercentage;
			if (basePercentage > 0 && testPercentage > 0) {
				lnTestDivBase = Math.log(testPercentage / basePercentage);
				psi = testSubBase * lnTestDivBase / 100;
			}
			testPercentage = FeatureBinsUtil.keepGivenDecimal(testPercentage, 2);
			basePercentage = FeatureBinsUtil.keepGivenDecimal(basePercentage, 2);
			testSubBase = FeatureBinsUtil.keepGivenDecimal(testSubBase, 2);
			lnTestDivBase = FeatureBinsUtil.keepGivenDecimal(lnTestDivBase, 2);
			psi = FeatureBinsUtil.keepGivenDecimal(psi, 4);
		}
	}

	public static class ScorecardVizData extends VizData {
		private static final long serialVersionUID = 260525523654434938L;
		public Double unscaledValue;
		public Double scaledValue;
		public Double woe;
		public Long total;
		public Long positive;
		public Long negative;
		public Double positiveRate;
		public Double negativeRate;

		ScorecardVizData() {}

		public ScorecardVizData(String featureName) {
			super(featureName);
		}

		public ScorecardVizData(String featureName, Long index, String value) {
			super(featureName, index, value);
		}
	}

	public static SerializableComparator <VizData> VizDataComparator = new SerializableComparator <VizData>() {
		private static final long serialVersionUID = 8715645640166932623L;

		@Override
		public int compare(VizData o1, VizData o2) {
			if (o1.index == null) {
				return -1;
			}
			if (o2.index == null) {
				return 1;
			}
			if (o1.index == -1) {
				return o2.index < -1 ? -1 : 1;
			}
			if (o1.index == -2) {
				return 1;
			}
			if (o2.index == -1) {
				return o1.index < -1 ? 1 : -1;
			}
			if (o2.index == -2) {
				return -1;
			}
			return o1.index.compareTo(o2.index);
		}
	};
}
