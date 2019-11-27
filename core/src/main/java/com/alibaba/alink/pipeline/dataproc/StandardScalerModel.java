package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.StandardScalerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.StandardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is model of StandardScaler, which is used in predict or transform.
 */
public class StandardScalerModel extends MapModel<StandardScalerModel>
	implements StandardPredictParams <StandardScalerModel> {

	public StandardScalerModel(Params params) {
		super(StandardScalerModelMapper::new, params);
	}

}