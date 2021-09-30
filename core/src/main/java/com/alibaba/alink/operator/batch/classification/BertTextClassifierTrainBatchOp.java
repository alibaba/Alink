package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.dl.BaseEasyTransferTrainBatchOp;
import com.alibaba.alink.common.dl.TaskType;
import com.alibaba.alink.common.dl.BertTaskName;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;

public class BertTextClassifierTrainBatchOp extends BaseEasyTransferTrainBatchOp <BertTextClassifierTrainBatchOp> {
	public BertTextClassifierTrainBatchOp() {
		this(new Params());
	}

	public BertTextClassifierTrainBatchOp(Params params) {
		super(params.clone()
			.set(HasTaskType.TASK_TYPE, TaskType.CLASSIFICATION)
			.set(HasTaskName.TASK_NAME, BertTaskName.TEXT_CLASSIFY));
	}
}
