package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossType;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.regression.GbdtRegTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Gradient Boosting(often abbreviated to GBDT or GBM) is a popular supervised learning model.
 * It is the best off-the-shelf supervised learning model for a wide range of problems,
 * especially problems with medium to large data size.
 * <p>
 * This implementation use histogram-based algorithm.
 * See:
 * "Mcrank: Learning to rank using multiple classification and gradient boosting", Ping Li et al., NIPS 2007,
 * for detail and experiments on histogram-based algorithm.
 * <p>
 * This implementation use layer-wise tree growing strategy,
 * rather than leaf-wise tree growing strategy
 * (like the one in "Lightgbm: A highly efficient gradient boosting decision tree", Guolin Ke et al., NIPS 2017),
 * because we found the former being faster in flink-based distributed computing environment.
 * <p>
 * This implementation use data-parallel algorithm.
 * See:
 * "A communication-efficient parallel algorithm for decision tree", Qi Meng et al., NIPS 2016
 * for an introduction on data-parallel, feature-parallel, etc., algorithms to construct decision forests.
 */
@NameCn("GBDT回归训练")
@NameEn("GBDT Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.GbdtRegressor")
public class GbdtRegTrainBatchOp extends BaseGbdtTrainBatchOp <GbdtRegTrainBatchOp>
	implements GbdtRegTrainParams <GbdtRegTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.GbdtModelInfo, GbdtRegTrainBatchOp, GbdtRegModelInfoBatchOp> {

	private static final long serialVersionUID = 6021957109313538690L;

	public GbdtRegTrainBatchOp() {
		this(null);
	}

	public GbdtRegTrainBatchOp(Params params) {
		super(params);

		getParams().set(LossUtils.LOSS_TYPE, LossType.LEASE_SQUARE);
	}

	@Override
	public GbdtRegModelInfoBatchOp getModelInfoBatchOp() {
		return new GbdtRegModelInfoBatchOp(getParams()).linkFrom(this);
	}

}
