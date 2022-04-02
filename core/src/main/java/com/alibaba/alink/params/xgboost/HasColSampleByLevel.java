package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasColSampleByLevel<T> extends WithParams <T> {

	@NameCn("每个树列采样")
	@DescCn("每个树列采样")
	ParamInfo <Double> COL_SAMPLE_BY_LEVEL = ParamInfoFactory
		.createParamInfo("colSampleByLevel", Double.class)
		.setDescription("The subsample ratio of columns for each level.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getColSampleByLevel() {
		return get(COL_SAMPLE_BY_LEVEL);
	}

	default T setColSampleByLevel(Double colSampleByTree) {
		return set(COL_SAMPLE_BY_LEVEL, colSampleByTree);
	}
}
