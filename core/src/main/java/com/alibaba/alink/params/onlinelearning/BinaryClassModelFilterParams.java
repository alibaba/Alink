package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

public interface BinaryClassModelFilterParams<T> extends WithParams <T>,
	HasLabelCol <T>,
	HasPositiveLabelValueString <T> {

	@NameCn("模型过滤的Auc阈值")
	@DescCn("模型过滤的Auc阈值")
	ParamInfo <Double> AUC_THRESHOLD = ParamInfoFactory
		.createParamInfo("aucThreshold", Double.class)
		.setDescription("auc threshold")
		.setHasDefaultValue(0.5)
		.build();

	default Double getAucThreshold() {return get(AUC_THRESHOLD);}

	default T setAucThreshold(Double value) {return set(AUC_THRESHOLD, value);}

	@NameCn("模型过滤的Accuracy阈值")
	@DescCn("模型过滤的Accuracy阈值")
	ParamInfo <Double> ACCURACY_THRESHOLD = ParamInfoFactory
		.createParamInfo("accuracyThreshold", Double.class)
		.setDescription("accuracy threshold")
		.setHasDefaultValue(0.5)
		.build();

	default Double getAccuracyThreshold() {return get(ACCURACY_THRESHOLD);}

	default T setAccuracyThreshold(Double value) {return set(ACCURACY_THRESHOLD, value);}


	@NameCn("模型过滤的LogLoss阈值")
	@DescCn("模型过滤的LogLoss阈值")
	ParamInfo <Double> LOG_LOSS_THRESHOLD = ParamInfoFactory
		.createParamInfo("logLossThreshold", Double.class)
		.setAlias(new String[]{"logLoss"})
		.setDescription("logLoss threshold")
		.setHasDefaultValue(Double.MAX_VALUE)
		.build();

	default Double getLogLossThreshold() {return get(LOG_LOSS_THRESHOLD);}

	default T setLogLossThreshold(Double value) {return set(LOG_LOSS_THRESHOLD, value);}

	@NameCn("评估样本数据条数")
	@DescCn("用多少样本数据对模型进行评估")
	ParamInfo <Integer> NUM_EVAL_SAMPLES = ParamInfoFactory
		.createParamInfo("numEvalSamples", Integer.class)
		.setDescription("num eval samples.")
		.setHasDefaultValue(200000)
		.build();

	default Integer getNumEvalSamples() {return get(NUM_EVAL_SAMPLES);}

	default T setNumEvalSamples(Integer value) {return set(NUM_EVAL_SAMPLES, value);}
}
