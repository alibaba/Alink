package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by BisectingKMeans.
 */
public class BisectingKMeansModel extends MapModel<BisectingKMeansModel>
	implements BisectingKMeansPredictParams <BisectingKMeansModel> {

	public BisectingKMeansModel() {this(null);}

	public BisectingKMeansModel(Params params) {
		super(BisectingKMeansModelMapper::new, params);
	}

}
