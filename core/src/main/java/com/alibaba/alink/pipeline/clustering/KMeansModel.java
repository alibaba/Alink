package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.params.clustering.KMeansPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by KMeans.
 */
@NameCn("K均值聚类模型")
public class KMeansModel extends MapModel <KMeansModel>
	implements KMeansPredictParams <KMeansModel> {

	private static final long serialVersionUID = -8633365072503327616L;

	public KMeansModel() {this(null);}

	public KMeansModel(Params params) {
		super(KMeansModelMapper::new, params);
	}

}
