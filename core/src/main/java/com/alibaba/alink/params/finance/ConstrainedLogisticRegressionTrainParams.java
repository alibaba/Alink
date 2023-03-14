package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasStandardization;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

public interface ConstrainedLogisticRegressionTrainParams<T>
	extends ConstrainedLinearModelParams <T>,
	HasWithIntercept <T>,
	HasMaxIterDefaultAs100 <T>,
	HasEpsilonDefaultAs0000001 <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasStandardization <T>,
	HasL1 <T>,
	HasL2 <T>,
	HasConstrainedOptimizationMethod <T> {
	@NameCn("正样本")
	@DescCn("正样本对应的字符串格式。")
	ParamInfo <String> POS_LABEL_VAL_STR = ParamInfoFactory
		.createParamInfo("positiveLabelValueString", String.class)
		.setDescription("positive label value with string format.")
		.setRequired()
		.setAlias(new String[] {"predPositiveLabelValueString", "positiveValue"})
		.build();

	default String getPositiveLabelValueString() {
		return get(POS_LABEL_VAL_STR);
	}

	default T setPositiveLabelValueString(String value) {
		return set(POS_LABEL_VAL_STR, value);
	}
}