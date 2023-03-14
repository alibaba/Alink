package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.dataproc.StandardPredictParams;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@NameCn("标准化")
public class StandardScaler extends Trainer <StandardScaler, StandardScalerModel> implements
	StandardTrainParams <StandardScaler>,
	StandardPredictParams <StandardScaler>,
	HasLazyPrintModelInfo <StandardScaler> {

	private static final long serialVersionUID = -2881876388698556871L;

	public StandardScaler() {
		super();
	}

	public StandardScaler(Params params) {
		super(params);
	}

}

