package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.recommendation.fm.HasLambda0DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLambda1DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLambda2DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLearnRateDefaultAs001;
import com.alibaba.alink.params.shared.HasVectorSize;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface OnlineFmTrainParams<T> extends
	HasLabelCol <T>,
	HasVectorSize <T>,
	HasTimeIntervalDefaultAs1800 <T>,
	HasVectorCol <T>,
	HasLambda0DefaultAs0 <T>,
	HasLambda1DefaultAs0 <T>,
	HasLambda2DefaultAs0 <T>,
	HasLearnRateDefaultAs001 <T> {

	@NameCn("全量模型输出频次")
	@DescCn("隔多少个增量模型输出一个全量模型")
	ParamInfo <Integer> TIMES_BETWEEN_FULL_MODEL_SAVED = ParamInfoFactory
		.createParamInfo("timesBetweenFullModelSaved", Integer.class)
		.setDescription("times between full model saved. when 0 is set, then all models are full model.")
		.setHasDefaultValue(0)
		.build();

	default Integer getTimesBetweenFullModelSaved() {return get(TIMES_BETWEEN_FULL_MODEL_SAVED);}

	default T setTimesBetweenFullModelSaved(Integer value) {return set(TIMES_BETWEEN_FULL_MODEL_SAVED, value);}

	@NameCn("优化方法")
	@DescCn("设置在线FM算法的优化方法：osgd、rms、adagrad。")
	ParamInfo <OptimMethod> OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("optimMethod", OptimMethod.class)
		.setDescription("optimize method")
		.setAlias(new String[] {"method"})
		.setHasDefaultValue(OptimMethod.OSGD)
		.build();

	default OptimMethod getOptimMethod() {
		return get(OPTIM_METHOD);
	}

	default T setOptimMethod(OptimMethod value) {
		return set(OPTIM_METHOD, value);
	}

	/**
	 * Optimization Type.
	 */
	enum OptimMethod {
		OSGD,
        ADAGRAD,
        RMS
    }
}
