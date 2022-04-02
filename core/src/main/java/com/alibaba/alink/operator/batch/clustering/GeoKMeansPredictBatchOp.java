package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.params.clustering.GeoKMeansPredictParams;

/**
 * GeoKMeans prediction based on the model fitted by GeoKMeansTrainBatchOp.
 */
@NameCn("经纬度K均值聚类预测")
public final class GeoKMeansPredictBatchOp extends ModelMapBatchOp <GeoKMeansPredictBatchOp>
	implements GeoKMeansPredictParams <GeoKMeansPredictBatchOp> {
	private static final long serialVersionUID = 2666865290298374230L;

	/**
	 * null constructor
	 */
	public GeoKMeansPredictBatchOp() {
		this(null);
	}

	/**
	 * constructor
	 *
	 * @param params
	 */
	public GeoKMeansPredictBatchOp(Params params) {
		super(KMeansModelMapper::new, params);
	}
}
