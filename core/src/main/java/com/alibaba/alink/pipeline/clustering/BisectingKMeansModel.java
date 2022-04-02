package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by BisectingKMeans.
 */
@NameCn("二分K均值聚类模型")
public class BisectingKMeansModel extends MapModel <BisectingKMeansModel>
	implements BisectingKMeansPredictParams <BisectingKMeansModel> {

	private static final long serialVersionUID = 8697075345055743984L;

	public BisectingKMeansModel() {this(null);}

	public BisectingKMeansModel(Params params) {
		super(BisectingKMeansModelMapper::new, params);
	}

}
