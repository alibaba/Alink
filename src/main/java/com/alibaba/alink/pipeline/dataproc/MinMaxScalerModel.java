package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is the model of MinMaxScaler, which wili be used in predict.
 */
public class MinMaxScalerModel extends MapModel<MinMaxScalerModel>
	implements MinMaxScalerPredictParams <MinMaxScalerModel> {

	public MinMaxScalerModel(Params params) {
		super(MinMaxScalerModelMapper::new, params);
	}

}