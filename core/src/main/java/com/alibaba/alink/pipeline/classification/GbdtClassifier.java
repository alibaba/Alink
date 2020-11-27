package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp;
import com.alibaba.alink.params.classification.GbdtPredictParams;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.pipeline.Trainer;

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
public class GbdtClassifier extends Trainer <GbdtClassifier, GbdtClassificationModel> implements
	GbdtTrainParams <GbdtClassifier>,
	GbdtPredictParams <GbdtClassifier>,
	HasLazyPrintModelInfo <GbdtClassifier> {

	private static final long serialVersionUID = 7228606857064008240L;

	public GbdtClassifier() {
		this(new Params());
	}

	public GbdtClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new GbdtTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
