package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelMapper;
import com.alibaba.alink.params.dataproc.MaxAbsScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * MaxAbsScaler pipeline model.
 */
public class MaxAbsScalerModel extends MapModel <MaxAbsScalerModel>
	implements MaxAbsScalerPredictParams <MaxAbsScalerModel> {

	private static final long serialVersionUID = 6472359926813867078L;

	public MaxAbsScalerModel(Params params) {
		super(MaxAbsScalerModelMapper::new, params);
	}
}