package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueStringDefaultAs1;

public interface ScorecardTrainParams<T>
	extends HasSelectedCols <T>,
	HasWeightColDefaultAsNull <T>,
	HasLabelCol <T>,
	HasPositiveLabelValueStringDefaultAs1 <T>,
	HasL1 <T>,
	HasL2 <T>,
	HasMaxIterDefaultAs100 <T>,
	HasEpsilonDefaultAs0000001 <T>,
	HasScaledValue <T>,
	HasOdds <T>,
	HasPdo <T>,
	HasDefaultWoe <T>,
	HasConstrainedOptimizationMethod <T>,
	HasConstrainedLinearModelType <T>,
	HasAlphaEntry <T>,
	HasAlphaStay <T> {

	@NameCn("编码方法")
	@DescCn("编码方法")
	ParamInfo <Encode> ENCODE = ParamInfoFactory
		.createParamInfo("encode", Encode.class)
		.setDescription("encode type: ASSEMBLED_VECTOR, WOE, NULL")
		.setHasDefaultValue(Encode.ASSEMBLED_VECTOR)
		.build();

	default Encode getEncode() {
		return get(ENCODE);
	}

	default T setEncode(Encode value) {
		return set(ENCODE, value);
	}

	default T setEncode(String value) {
		return set(ENCODE, ParamUtil.searchEnum(ENCODE, value));
	}

	/**
	 * Encode type for Binning.
	 */
	enum Encode {
		/**
		 * Output the woe of the bin.
		 */
		WOE,

		/**
		 * If there are multi columns, first encode these columns as vectors, and output the assembled vector.
		 */
		ASSEMBLED_VECTOR,

		/**
		 * The bin is not encoded, return the data as it is.
		 */
		NULL
	}

	@NameCn("是否将模型进行分数转换")
	@DescCn("是否将模型进行分数转换")
	ParamInfo <Boolean> SCALE_INFO = ParamInfoFactory
		.createParamInfo("scaleInfo", Boolean.class)
		.setDescription("scaleInfo")
		.setHasDefaultValue(false)
		.build();

	default Boolean getScaleInfo() {
		return get(SCALE_INFO);
	}

	default T setScaleInfo(Boolean value) {
		return set(SCALE_INFO, value);
	}

	@NameCn("是否逐步回归")
	@DescCn("是否逐步回归")
	ParamInfo <Boolean> WITH_SELECTOR = ParamInfoFactory
		.createParamInfo("withSelector", Boolean.class)
		.setDescription("if feature selector or not.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getWithSelector() {
		return get(WITH_SELECTOR);
	}

	default T setWithSelector(Boolean value) {
		return set(WITH_SELECTOR, value);
	}

	@NameCn("强制选择的列")
	@DescCn("强制选择的列")
	ParamInfo <String[]> FORCE_SELECTED_COLS = ParamInfoFactory
		.createParamInfo("forceSelectedCols", String[].class)
		.setDescription("force selected cols.")
		.setOptional()
		.build();

	default String[] getForceSelectedCols() {
		return get(FORCE_SELECTED_COLS);
	}

	default T setForceSelectedCols(String[] value) {
		return set(FORCE_SELECTED_COLS, value);
	}

}
