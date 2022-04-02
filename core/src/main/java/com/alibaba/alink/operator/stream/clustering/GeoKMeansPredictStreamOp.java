package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.GeoKMeansPredictParams;

/**
 * Find  the closest cluster center for every point.
 */
@NameCn("经纬度K均值聚类预测")
public final class GeoKMeansPredictStreamOp
	extends ModelMapStreamOp <GeoKMeansPredictStreamOp>
	implements GeoKMeansPredictParams <GeoKMeansPredictStreamOp> {

	private static final long serialVersionUID = 2094531381894465793L;

	/**
	 * default constructor
	 *
	 * @param model trained from kMeansBatchOp
	 */
	public GeoKMeansPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public GeoKMeansPredictStreamOp(BatchOperator model, Params params) {
		super(model, KMeansModelMapper::new, params);
	}
}
