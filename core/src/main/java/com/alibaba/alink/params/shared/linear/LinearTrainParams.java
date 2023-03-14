package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.optim.HasLearningRateDefaultAs01;

/**
 * parameters of linear training.
 */
public interface LinearTrainParams<T> extends
	HasWithIntercept <T>,
	HasMaxIterDefaultAs100 <T>,
	HasEpsilonDefaultAs0000001 <T>,
	HasLearningRateDefaultAs01 <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasStandardization <T> {

	@NameCn("优化方法")
	@DescCn("优化问题求解时选择的优化方法")
	ParamInfo <OptimMethod> OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("optimMethod", OptimMethod.class)
		.setDescription("optimization method")
		.setHasDefaultValue(null)
		.build();

	default OptimMethod getOptimMethod() {
		return get(OPTIM_METHOD);
	}

	default T setOptimMethod(String value) {
		return set(OPTIM_METHOD, ParamUtil.searchEnum(OPTIM_METHOD, value));
	}

	default T setOptimMethod(OptimMethod value) {
		return set(OPTIM_METHOD, value);
	}

	/**
	 * Optimization Type.
	 */
	enum OptimMethod {
		LBFGS,
		GD,
		Newton,
		SGD,
		OWLQN
	}
}
