package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.dl.BaseEasyTransferTrainBatchOp;
import com.alibaba.alink.common.dl.BertTaskName;
import com.alibaba.alink.common.dl.TaskType;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.tensorflow.bert.BertTextPairTrainParams;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a text pair regressor using Bert models.
 */
@ParamSelectColumnSpec(name = "textCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@ParamSelectColumnSpec(name = "textPairCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("Bert文本对回归训练")
@NameEn("Bert Text Pair Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.BertTextPairRegressor")
public class BertTextPairRegressorTrainBatchOp extends BaseEasyTransferTrainBatchOp <BertTextPairRegressorTrainBatchOp>
	implements BertTextPairTrainParams <BertTextPairRegressorTrainBatchOp> {

	public BertTextPairRegressorTrainBatchOp() {
		this(new Params());
	}

	public BertTextPairRegressorTrainBatchOp(Params params) {
		super(params.clone()
			.set(HasTaskType.TASK_TYPE, TaskType.REGRESSION)
			.set(HasTaskName.TASK_NAME, BertTaskName.TEXT_MATCH));
	}
}
