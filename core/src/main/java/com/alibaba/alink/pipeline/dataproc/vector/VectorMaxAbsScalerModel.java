package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is the model for MaxAbs predicting.
 */
public class VectorMaxAbsScalerModel extends MapModel<VectorMaxAbsScalerModel>
	implements VectorMaxAbsScalerPredictParams <VectorMaxAbsScalerModel> {

	public VectorMaxAbsScalerModel(Params params) {
		super(VectorMaxAbsScalerModelMapper::new, params);
	}
}