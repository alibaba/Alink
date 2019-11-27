package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.MaxAbsScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is the model of MaxAbsScaler, which wili be used in predict or transform.
 */
public class MaxAbsScalerModel extends MapModel<MaxAbsScalerModel>
	implements MaxAbsScalerPredictParams <MaxAbsScalerModel> {

	public MaxAbsScalerModel(Params params) {
		super(MaxAbsScalerModelMapper::new, params);
	}
}