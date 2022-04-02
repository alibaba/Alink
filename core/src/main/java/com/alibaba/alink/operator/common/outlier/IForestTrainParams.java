package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaultAs100;
import com.alibaba.alink.params.validators.RangeValidator;

public interface IForestTrainParams<T> extends
	WithMultiVarParams <T>,
	HasNumTreesDefaultAs100 <T> {

	@NameCn("每棵树的样本采样行数")
	@DescCn("每棵树的样本采样行数，默认 256 ，最小 2 ，最大 100000 .")
	ParamInfo <Integer> SUBSAMPLING_SIZE = ParamInfoFactory
		.createParamInfo("subsamplingSize", Integer.class)
		.setDescription("Size of the training samples used for learning each isolation tree.")
		.setHasDefaultValue(256)
		.setValidator(new RangeValidator <>(1, 100000))
		.build();

	default Integer getSubsamplingSize() {
		return get(SUBSAMPLING_SIZE);
	}

	default T setSubsamplingSize(Integer size) {
		return set(SUBSAMPLING_SIZE, size);
	}
}
