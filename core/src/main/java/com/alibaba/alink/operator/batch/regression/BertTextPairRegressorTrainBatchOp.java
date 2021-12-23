package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.dl.BaseEasyTransferTrainBatchOp;
import com.alibaba.alink.common.dl.TaskType;
import com.alibaba.alink.common.dl.BertTaskName;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.tensorflow.bert.BertTextPairTrainParams;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;

/**
 * Train a text pair regressor using Bert models.
 */
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
