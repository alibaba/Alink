package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorImputerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorImputerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is Imputer model for Imputer predict.
 */
public class VectorImputerModel extends MapModel <VectorImputerModel>
	implements VectorImputerPredictParams <VectorImputerModel> {

	private static final long serialVersionUID = 8247341225136981166L;

	public VectorImputerModel(Params params) {
		super(VectorImputerModelMapper::new, params);
	}
}