package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;
import com.alibaba.alink.params.clustering.KModesTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * @author guotao.gt
 */
@NameCn("Kmodes训练")
public class KModes extends Trainer <KModes, KModesModel> implements
	KModesTrainParams <KModes>,
	ClusteringPredictParams <KModes> {

	private static final long serialVersionUID = 7852949697147507989L;

	public KModes() {
		super();
	}

	public KModes(Params params) {
		super(params);
	}

}
