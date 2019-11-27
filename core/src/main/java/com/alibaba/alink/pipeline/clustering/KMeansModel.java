package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.clustering.KMeansPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Find  the closest cluster center for every point.
 */
public class KMeansModel extends MapModel<KMeansModel>
	implements KMeansPredictParams <KMeansModel> {

	public KMeansModel() {this(null);}

	public KMeansModel(Params params) {
		super(KMeansModelMapper::new, params);
	}

}
