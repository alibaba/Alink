package com.alibaba.alink.params.shared.linear;

import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of linear training.
 *
 */
public interface LinearTrainParams<T> extends
	HasWithIntercept <T>,
	HasMaxIterDefaultAs100<T>,
	HasEpsilonDv0000001 <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasStandardization <T> {

	ParamInfo<String> OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("optimMethod", String.class)
		.setDescription("optimization method")
		.setHasDefaultValue(null)
		.build();

	default String getOptimMethod() {
		return get(OPTIM_METHOD);
	}

	default T setOptimMethod(String value) {
		return set(OPTIM_METHOD, value);
	}
}
