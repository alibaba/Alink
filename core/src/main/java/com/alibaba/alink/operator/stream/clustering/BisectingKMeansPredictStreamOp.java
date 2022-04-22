package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;

/**
 * Bisecting KMeans prediction based on the model fitted by BisectingKMeansTrainBatchOp.
 */
@NameCn("二分K均值聚类预测")
public final class BisectingKMeansPredictStreamOp extends ModelMapStreamOp <BisectingKMeansPredictStreamOp>
	implements BisectingKMeansPredictParams <BisectingKMeansPredictStreamOp> {

	private static final long serialVersionUID = 5540690973953730811L;

	public BisectingKMeansPredictStreamOp() {
		super(BisectingKMeansModelMapper::new, new Params());
	}

	public BisectingKMeansPredictStreamOp(Params params) {
		super(BisectingKMeansModelMapper::new, params);
	}

	public BisectingKMeansPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public BisectingKMeansPredictStreamOp(BatchOperator model, Params params) {
		super(model, BisectingKMeansModelMapper::new, params);
	}
}
