package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.params.clustering.GeoKMeansPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by GeoKMeans.
 */
@NameCn("经纬度K均值聚类模型")
public class GeoKMeansModel extends MapModel <GeoKMeansModel>
	implements GeoKMeansPredictParams <GeoKMeansModel> {

	private static final long serialVersionUID = 5440299021941167875L;

	public GeoKMeansModel() {this(null);}

	public GeoKMeansModel(Params params) {
		super(KMeansModelMapper::new, params);
	}

}
