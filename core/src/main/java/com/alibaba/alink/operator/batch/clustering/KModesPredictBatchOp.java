package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.kmodes.KModesModelMapper;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;

/**
 * @author guotao.gt
 */

@NameCn("Kmodes预测")
@NameEn("KModes Prediction")
public final class KModesPredictBatchOp extends ModelMapBatchOp <KModesPredictBatchOp>
	implements ClusteringPredictParams <KModesPredictBatchOp> {

	private static final long serialVersionUID = -893588697092734428L;

	/**
	 * null constructor
	 */
	public KModesPredictBatchOp() {
		this(null);
	}

	public KModesPredictBatchOp(Params params) {
		super(KModesModelMapper::new, params);
	}

}
