package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.GbdtModelInfo;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossType;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.classification.GbdtTrainParams;

/**
 * Fit a binary classfication model.
 *
 * @see BaseGbdtTrainBatchOp
 */
public final class GbdtTrainBatchOp extends BaseGbdtTrainBatchOp <GbdtTrainBatchOp>
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
