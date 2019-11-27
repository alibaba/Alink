package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is the model for MinMax predicting.
 */
public class VectorMinMaxScalerModel extends MapModel<VectorMinMaxScalerModel>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerModel> {

	public VectorMinMaxScalerModel(Params params) {
		super(VectorMinMaxScalerModelMapper::new, params);
	}
}