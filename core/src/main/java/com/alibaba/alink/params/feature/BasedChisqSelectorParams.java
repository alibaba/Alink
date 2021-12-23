package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasLabelCol;

/**
 * Trait for ChisqSelector and VectorChisqSelector.
 */
public interface BasedChisqSelectorParams<T> extends
	HasLabelCol <T> {

	/**
	 * @cn-name 筛选类型
	 * @cn 筛选类型，包含"NumTopFeatures","percentile", "fpr", "fdr", "fwe"五种。
	 */
	ParamInfo <SelectorType> SELECTOR_TYPE = ParamInfoFactory.createParamInfo("selectorType",
		SelectorType.class)
		.setDescription("The selector supports different selection methods: `NumTopFeatures`, `percentile`, `fpr`,\n" +
			"  `fdr`, `fwe`.\n" +
			"   - `NumTopFeatures` chooses a fixed number of top features according to a chi-squared test.\n" +
			"   - `percentile` is similar but chooses a fraction of all features instead of a fixed number.\n" +
			"   - `fpr` chooses all features whose p-values are below a threshold, thus controlling the false\n" +
			"     positive rate of selection.\n" +
			"   - `fdr` uses the [Benjamini-Hochberg procedure]\n" +
			"     (https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure)\n" +
			"     to choose all features whose false discovery rate is below a threshold.\n" +
			"   - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by\n" +
			"     1/numFeatures, thus controlling the family-wise error rate of selection.\n" +
			"  By default, the selection method is `NumTopFeatures`, with the default number of top features")
		.setOptional()
		.setHasDefaultValue(SelectorType.NumTopFeatures)
		.build();
	/**
	 * @cn-name 最大的p-value列个数
	 * @cn 最大的p-value列个数, 默认值50
	 */
	ParamInfo <Integer> NUM_TOP_FEATURES = ParamInfoFactory.createParamInfo("numTopFeatures", Integer.class)
		.setDescription("Number of features that selector will select, ordered by ascending p-value. If the" +
			" number of features is < NumTopFeatures, then this will select all features." +
			"  By default, 50")
		.setOptional()
		.setHasDefaultValue(50)
		.build();
	/**
	 * @cn-name 筛选的百分比
	 * @cn 筛选的百分比，默认值0.1
	 */
	ParamInfo <Double> PERCENTILE = ParamInfoFactory.createParamInfo("percentile", Double.class)
		.setDescription(
			"Percentile of features that selector will select, ordered by ascending p-value. It must be in range (0,1)"
				+
				"  By default, 0.1")
		.setOptional()
		.setHasDefaultValue(0.1)
		.build();
	/**
	 * @cn-name p value的阈值
	 * @cn p value的阈值，默认值0.05
	 */
	ParamInfo <Double> FPR = ParamInfoFactory.createParamInfo("fpr", Double.class)
		.setDescription("The highest p-value for features to be kept. It must be in range (0,1)" +
			"  By default, 0.05")
		.setHasDefaultValue(0.05)
		.build();
	/**
	 * @cn-name 发现阈值
	 * @cn 发现阈值, 默认值0.05
	 */
	ParamInfo <Double> FDR = ParamInfoFactory.createParamInfo("fdr", Double.class)
		.setDescription("The upper bound of the expected false discovery rate.It must be in range (0,1)" +
			"  By default, 0.05")
		.setHasDefaultValue(0.05)
		.build();
	/**
	 * @cn-name 错误率阈值
	 * @cn 错误率阈值, 默认值0.05
	 */
	ParamInfo <Double> FWE = ParamInfoFactory.createParamInfo("fwe", Double.class)
		.setDescription("The upper bound of the expected family-wise error rate. rate.It must be in range (0,1)" +
			"  By default, 0.05")
		.setHasDefaultValue(0.05)
		.build();

	default SelectorType getSelectorType() {
		return get(SELECTOR_TYPE);
	}

	default T setSelectorType(SelectorType value) {
		return set(SELECTOR_TYPE, value);
	}

	default T setSelectorType(String value) {
		return set(SELECTOR_TYPE, ParamUtil.searchEnum(SELECTOR_TYPE, value));
	}

	default Integer getNumTopFeatures() {
		return get(NUM_TOP_FEATURES);
	}

	default T setNumTopFeatures(Integer value) {
		return set(NUM_TOP_FEATURES, value);
	}

	default Double getPercentile() {
		return get(PERCENTILE);
	}

	default T setPercentile(Double value) {
		return set(PERCENTILE, value);
	}

	default Double getFpr() {
		return get(FPR);
	}

	default T setFpr(Double value) {
		return set(FPR, value);
	}

	default Double getFdr() {
		return get(FDR);
	}

	default T setFdr(Double value) {
		return set(FDR, value);
	}

	default Double getFwe() {
		return get(FWE);
	}

	default T setFwe(Double value) {
		return set(FWE, value);
	}

	/**
	 * chi-square selector type.
	 */

	enum SelectorType {
		/**
		 * select NumTopFeatures features which are maximum chi-square value.
		 */
		NumTopFeatures,

		/**
		 * select percentile * n features which are maximum chi-square value.
		 */
		PERCENTILE,

		/**
		 * select feature which chi-square value less than fpr.
		 */
		FPR,

		/**
		 * select feature which chi-square value less than fdr * (i + 1) / n.
		 */
		FDR,

		/**
		 * select feature which chi-square value less than fwe / n.
		 */
		FWE
	}
}
