package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Vector MinMax pipeline model.
 */
public class VectorMinMaxScalerModel extends MapModel <VectorMinMaxScalerModel>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerModel> {

	private static final long serialVersionUID = 1484050918824702606L;

	public VectorMinMaxScalerModel(Params params) {
		super(VectorMinMaxScalerModelMapper::new, params);
	}
}