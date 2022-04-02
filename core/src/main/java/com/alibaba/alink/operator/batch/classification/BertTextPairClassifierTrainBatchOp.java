package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.dl.BaseEasyTransferTrainBatchOp;
import com.alibaba.alink.common.dl.BertTaskName;
import com.alibaba.alink.common.dl.TaskType;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.tensorflow.bert.BertTextPairTrainParams;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;

/**
 * Train a text pair classifier using Bert models.
 */
@ParamSelectColumnSpec(name = "textCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@ParamSelectColumnSpec(name = "textPairCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("Bert文本对分类训练")
public class BertTextPairClassifierTrainBatchOp
	extends BaseEasyTransferTrainBatchOp <BertTextPairClassifierTrainBatchOp>
	implements BertTextPairTrainParams <BertTextPairClassifierTrainBatchOp> {

	public BertTextPairClassifierTrainBatchOp() {
		this(new Params());
	}

	public BertTextPairClassifierTrainBatchOp(Params params) {
		super(params.clone()
			.set(HasTaskType.TASK_TYPE, TaskType.CLASSIFICATION)
			.set(HasTaskName.TASK_NAME, BertTaskName.TEXT_MATCH));
	}
}
