package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.finance.HasBinningMethod;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.validators.MinValidator;
import com.alibaba.alink.params.validators.RangeValidator;

/**
 * Params for autocross.
 */
public interface AutoCrossTrainParams<T> extends
	HasSelectedCols <T>,
	HasCategoricalCols<T>,
	HasLabelCol <T>,
	HasDiscreteThresholds <T>,
	HasDiscreteThresholdsArray <T>,
	HasNumBuckets <T>,
	HasNumBucketsArray <T>,
	HasBinningMethod <T> {

	@NameCn("特征组合搜索步数")
	@DescCn("特征组合搜索步数")
	ParamInfo <Integer> MAX_SEARCH_STEP = ParamInfoFactory
		.createParamInfo("maxSearchStep", Integer.class)
		.setDescription("Max search step.")
		.setHasDefaultValue(2)
		.build();

	default Integer getMaxSearchStep() {
		return get(MAX_SEARCH_STEP);
	}

	default T setMaxSearchStep(Integer value) {
		return set(MAX_SEARCH_STEP, value);
	}

	ParamInfo <Double> FRACTION = ParamInfoFactory
		.createParamInfo("fraction", Double.class)
		.setDescription("Fraction of train data.")
		.setHasDefaultValue(0.8)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getFraction() {
		return get(FRACTION);
	}

	default T setFraction(Double colName) {
		return set(FRACTION, colName);
	}

	ParamInfo <Boolean> FIX_COEFS = ParamInfoFactory
		.createParamInfo("fixCoefs", Boolean.class)
		.setDescription("fixCoefs")
		.setHasDefaultValue(false)
		.build();

	default Boolean getFixCoefs() {
		return get(FIX_COEFS);
	}

	default T setFixCoefs(Boolean value) {
		return set(FIX_COEFS, value);
	}

	ParamInfo <Integer> K_CROSS = ParamInfoFactory
		.createParamInfo("kCross", Integer.class)
		.setDescription("discreteThreshold")
		.setHasDefaultValue(1)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getKCross() {
		return get(K_CROSS);
	}

	default T setKCross(Integer value) {
		return set(K_CROSS, value);
	}

}
