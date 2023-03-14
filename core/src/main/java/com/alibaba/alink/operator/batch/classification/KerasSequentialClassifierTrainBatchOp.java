package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.dl.BaseKerasSequentialTrainBatchOp;
import com.alibaba.alink.common.dl.TaskType;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a classifier using a Keras Sequential model.
 */
@NameCn("KerasSequential分类训练")
@NameEn("KerasSequential Classifier Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.KerasSequentialClassifier")
public class KerasSequentialClassifierTrainBatchOp extends
	BaseKerasSequentialTrainBatchOp <KerasSequentialClassifierTrainBatchOp> {
	public KerasSequentialClassifierTrainBatchOp() {
		this(new Params());
	}

	public KerasSequentialClassifierTrainBatchOp(Params params) {
		super(params.clone().set(HasTaskType.TASK_TYPE, TaskType.CLASSIFICATION));
	}
}
