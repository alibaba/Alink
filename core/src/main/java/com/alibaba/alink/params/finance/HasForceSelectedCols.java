package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasForceSelectedCols<T> extends WithParams <T> {

	@NameCn("强制选择的列")
	@DescCn("强制选择的列")
	ParamInfo <int[]> FORCE_SELECTED_COLS = ParamInfoFactory
		.createParamInfo("forceSelectedCols", int[].class)
		.setDescription("force selected cols.")
		.setOptional()
		.build();

	default int[] getForceSelectedCols() {
		return get(FORCE_SELECTED_COLS);
	}

	default T setForceSelectedCols(int[] value) {
		return set(FORCE_SELECTED_COLS, value);
	}
}
