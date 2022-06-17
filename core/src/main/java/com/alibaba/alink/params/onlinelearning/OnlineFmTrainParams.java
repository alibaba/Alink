package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.recommendation.fm.HasLambda0DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLambda1DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLambda2DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLinearItemDefaultAsTrue;
import com.alibaba.alink.params.recommendation.fm.HasNumFactorsDefaultAs10;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

public interface OnlineFmTrainParams<T> extends
	ModelStreamScanParams <T>,
	HasLabelCol <T>,
	HasTimeIntervalDefaultAs1800 <T>,
	HasVectorColDefaultAsNull <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasL1DefaultAs01 <T>,
	HasL2DefaultAs01 <T>,
	HasAlpha <T>,
	HasBeta <T>,
	HasWithIntercept <T>,
	HasLinearItemDefaultAsTrue <T>,
	HasNumFactorsDefaultAs10 <T>,
	HasLambda0DefaultAs0 <T>,
	HasLambda1DefaultAs0 <T>,
	HasLambda2DefaultAs0 <T> {

	@NameCn("Batch大小")
	@DescCn("表示单次OnlineFM单次迭代更新使用的样本数量，建议是并行度的整数倍.")
	ParamInfo <Integer> MINI_BATCH_SIZE = ParamInfoFactory
		.createParamInfo("miniBatchSize", Integer.class)
		.setDescription("mini batch size")
		.setHasDefaultValue(512)
		.build();

	default Integer getMiniBatchSize() {return get(MINI_BATCH_SIZE);}

	default T setMiniBatchSize(Integer value) {return set(MINI_BATCH_SIZE, value);}
}