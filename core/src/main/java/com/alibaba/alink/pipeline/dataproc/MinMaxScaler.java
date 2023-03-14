package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;
import com.alibaba.alink.params.dataproc.MinMaxScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 */
@NameCn("归一化")
public class MinMaxScaler extends Trainer <MinMaxScaler, MinMaxScalerModel> implements
	MinMaxScalerTrainParams <MinMaxScaler>,
	MinMaxScalerPredictParams <MinMaxScaler>,
	HasLazyPrintModelInfo <MinMaxScaler> {

	private static final long serialVersionUID = 6773065743562180147L;

	public MinMaxScaler() {
		super();
	}

	public MinMaxScaler(Params params) {
		super(params);
	}

}

