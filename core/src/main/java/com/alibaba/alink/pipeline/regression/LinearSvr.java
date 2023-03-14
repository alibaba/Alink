package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.params.regression.LinearSvrPredictParams;
import com.alibaba.alink.params.regression.LinearSvrTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * @author yangxu
 */
@NameCn("线性SVR")
public class LinearSvr extends Trainer <LinearSvr, LinearSvrModel> implements
	LinearSvrTrainParams <LinearSvr>,
	LinearSvrPredictParams <LinearSvr>,
	HasLazyPrintTrainInfo <LinearSvr>, HasLazyPrintModelInfo <LinearSvr> {

	private static final long serialVersionUID = -2558162451984705090L;

	public LinearSvr() {
		super(new Params());
	}

	public LinearSvr(Params params) {
		super(params);
	}

}
