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
import com.alibaba.alink.params.tensorflow.bert.BertTextTrainParams;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a text regressor using Bert models.
 */
@ParamSelectColumnSpec(name = "textCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("Bert文本回归训练")
@NameEn("Bert Text Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.BertTextRegressor")
public class BertTextRegressorTrainBatchOp extends BaseEasyTransferTrainBatchOp <BertTextRegressorTrainBatchOp>
	implements BertTextTrainParams <BertTextRegressorTrainBatchOp> {

	public BertTextRegressorTrainBatchOp() {
		this(new Params());
	}

	public BertTextRegressorTrainBatchOp(Params params) {
		super(params.clone()
			.set(HasTaskType.TASK_TYPE, TaskType.REGRESSION)
			.set(HasTaskName.TASK_NAME, BertTaskName.TEXT_CLASSIFY));
	}
}
