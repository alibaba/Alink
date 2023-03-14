package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.operator.common.clustering.kmodes.KModesModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;

/**
 * @author guotao.gt
 */

@NameCn("Kmodes预测")
@NameEn("KModes Prediction")
public final class KModesPredictStreamOp extends ModelMapStreamOp <KModesPredictStreamOp>
	implements ClusteringPredictParams <KModesPredictStreamOp> {

	private static final long serialVersionUID = 8807678261602859709L;

	public KModesPredictStreamOp() {
		super(KModesModelMapper::new, new Params());
	}

	public KModesPredictStreamOp(Params params) {
		super(KModesModelMapper::new, params);
	}

	/**
	 * default constructor
	 *
	 * @param model train from kMeansBatchOp
	 */
	public KModesPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public KModesPredictStreamOp(BatchOperator model, Params params) {
		super(model, KModesModelMapper::new, params);
	}

}
