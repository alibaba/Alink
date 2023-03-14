package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.GbdtModelInfo;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossType;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Fit a binary classfication model.
 *
 * @see BaseGbdtTrainBatchOp
 */
@NameCn("GBDT分类器训练")
@NameEn("GBDT Classifier Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.GbdtClassifier")
public class GbdtTrainBatchOp extends BaseGbdtTrainBatchOp <GbdtTrainBatchOp>
	implements GbdtTrainParams <GbdtTrainBatchOp>,
	WithModelInfoBatchOp <GbdtModelInfo, GbdtTrainBatchOp, GbdtModelInfoBatchOp> {

	private static final long serialVersionUID = -2958514262060737173L;

	public GbdtTrainBatchOp() {
		this(null);
	}

	public GbdtTrainBatchOp(Params params) {
		super(params);
		getParams().set(LossUtils.LOSS_TYPE, LossType.LOG_LOSS);
	}

	@Override
	public GbdtModelInfoBatchOp getModelInfoBatchOp() {
		return new GbdtModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
