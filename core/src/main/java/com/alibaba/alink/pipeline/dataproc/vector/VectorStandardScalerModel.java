package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Vector standardization pipeline model.
 */
public class VectorStandardScalerModel extends MapModel <VectorStandardScalerModel>
	implements VectorStandardPredictParams <VectorStandardScalerModel> {

	private static final long serialVersionUID = 2721563131256274875L;

	public VectorStandardScalerModel(Params params) {
		super(VectorStandardScalerModelMapper::new, params);
	}
}