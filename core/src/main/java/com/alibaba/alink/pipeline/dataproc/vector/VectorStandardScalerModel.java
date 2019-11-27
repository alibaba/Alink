package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is the model for standardization predicting.
 */
public class VectorStandardScalerModel extends MapModel<VectorStandardScalerModel>
	implements VectorStandardPredictParams <VectorStandardScalerModel> {

	public VectorStandardScalerModel(Params params) {
		super(VectorStandardScalerModelMapper::new, params);
	}
}