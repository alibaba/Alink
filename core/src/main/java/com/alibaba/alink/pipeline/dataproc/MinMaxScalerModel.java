package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * MinMaxScaler pipeline model.
 */
@NameCn("归一化模型")
public class MinMaxScalerModel extends MapModel <MinMaxScalerModel>
	implements MinMaxScalerPredictParams <MinMaxScalerModel> {

	private static final long serialVersionUID = 810893780616289151L;

	public MinMaxScalerModel(Params params) {
		super(MinMaxScalerModelMapper::new, params);
	}

}
