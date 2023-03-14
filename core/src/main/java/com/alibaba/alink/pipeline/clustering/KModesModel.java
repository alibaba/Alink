package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.kmodes.KModesModelMapper;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("Kmodes模型")
public class KModesModel extends MapModel <KModesModel>
	implements ClusteringPredictParams <KModesModel> {

	private static final long serialVersionUID = 3952106117818656248L;

	public KModesModel() {this(null);}

	public KModesModel(Params params) {
		super(KModesModelMapper::new, params);
	}

}
