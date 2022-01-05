package com.alibaba.alink.params.tensorflow.kerasequential;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.dl.HasBatchSizeDefaultAs128;
import com.alibaba.alink.params.dl.HasCheckpointFilePathDefaultAsNull;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasLearningRateDefaultAs0001;
import com.alibaba.alink.params.dl.HasNumEpochsDefaultAs10;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.image.HasTensorCol;
import com.alibaba.alink.params.shared.colname.HasLabelCol;

public interface BaseKerasSequentialTrainParams<T> extends
	HasTensorCol <T>, HasLabelCol <T>,
	HasLayers <T>, HasOptimizer <T>,
	HasLearningRateDefaultAs0001 <T>,
	HasNumEpochsDefaultAs10 <T>, HasBatchSizeDefaultAs128 <T>, HasCheckpointFilePathDefaultAsNull <T>,
	HasPythonEnv <T>, HasIntraOpParallelism <T>,
	HasNumWorkersDefaultAsNull <T>, HasNumPssDefaultAsNull <T> {

	/**
	 * @cn-name 验证集比例
	 * @cn 验证集比例，当前需要总并发度为 1
	 */
	ParamInfo <Double> VALIDATION_SPLIT = ParamInfoFactory
		.createParamInfo("validationSplit", Double.class)
		.setDescription("Split ratio for validation set, currently only works when total parallelism is 1")
		.setHasDefaultValue(0.)
		.build();

	default Double getValidationSplit() {
		return get(VALIDATION_SPLIT);
	}

	default T setValidationSplit(Double value) {
		return set(VALIDATION_SPLIT, value);
	}

	/**
	 * @cn-name 是否导出最优的 checkpoint
	 * @cn 是否导出最优的 checkpoint
	 */
	ParamInfo <Boolean> SAVE_BEST_ONLY = ParamInfoFactory
		.createParamInfo("saveBestOnly", Boolean.class)
		.setDescription("Whether to export the checkpoint with best metric")
		.setHasDefaultValue(false)
		.build();

	default Boolean getSaveBestOnly() {
		return get(SAVE_BEST_ONLY);
	}

	default T setSaveBestOnly(Boolean value) {
		return set(SAVE_BEST_ONLY, value);
	}

	/**
	 * @cn-name 指标
	 * @cn 判断模型最优时用的指标。
	 * 都支持的有：loss；
	 * 二分类还支持：auc, precision, recall, binary_accuracy, false_negatives, false_positives, true_negatives, true_positives
	 * 多分类还支持：sparse_categorical_accuracy
	 * 回归还支持：mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, mean_squared_logarithmic_error, root_mean_squared_error
	 */
	ParamInfo <String> BEST_METRIC = ParamInfoFactory
		.createParamInfo("bestMetric", String.class)
		.setDescription("The metrics used to decide best checkpoint")
		.setHasDefaultValue("loss")
		.build();

	default String getBestMetric() {
		return get(BEST_METRIC);
	}

	default T setBestMetric(String value) {
		return set(BEST_METRIC, value);
	}

	/**
	 * @cn-name 每隔多少 epochs 保存 checkpoints
	 * @cn 每隔多少 epochs 保存 checkpoints
	 */
	ParamInfo <Double> SAVE_CHECKPOINTS_EPOCHS = ParamInfoFactory
		.createParamInfo("saveCheckpointsEpochs", Double.class)
		.setDescription("Save checkpoints every several epochs")
		.setHasDefaultValue(1.)
		.build();

	default Double getSaveCheckpointsEpochs() {
		return get(SAVE_CHECKPOINTS_EPOCHS);
	}

	default T setSaveCheckpointsEpochs(Double value) {
		return set(SAVE_CHECKPOINTS_EPOCHS, value);
	}

	/**
	 * @cn-name 每隔多少秒保存 checkpoints
	 * @cn 每隔多少秒保存 checkpoints
	 */
	ParamInfo <Double> SAVE_CHECKPOINTS_SECS = ParamInfoFactory
		.createParamInfo("saveCheckpointsSecs", Double.class)
		.setDescription("Save checkpoints every several seconds")
		.setOptional()
		.build();

	default Double getSaveCheckpointsSecs() {
		return get(SAVE_CHECKPOINTS_SECS);
	}

	default T setSaveCheckpointsSecs(Double value) {
		return set(SAVE_CHECKPOINTS_SECS, value);
	}
}
