package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.dl.BaseKerasSequentialTrainBatchOp;
import com.alibaba.alink.common.dl.TaskType;
import com.alibaba.alink.params.dl.HasTaskType;

/**
 * Train a regressor using a Keras Sequential model.
 */
@NameCn("KerasSequential回归训练")
public class KerasSequentialRegressorTrainBatchOp
	extends BaseKerasSequentialTrainBatchOp <KerasSequentialRegressorTrainBatchOp> {
	public KerasSequentialRegressorTrainBatchOp() {
		this(new Params());
	}

	public KerasSequentialRegressorTrainBatchOp(Params params) {
		super(params.clone().set(HasTaskType.TASK_TYPE, TaskType.REGRESSION));
	}
}
