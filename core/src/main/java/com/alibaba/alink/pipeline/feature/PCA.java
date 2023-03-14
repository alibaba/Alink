package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.feature.PcaPredictParams;
import com.alibaba.alink.params.feature.PcaTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * PCA is dimension reduction of discrete feature, projects vectors to a low-dimensional space.
 * PcaTrainBatchOp is train a model which can be used to batch predict and stream predict
 * The calculation is done using eigen on the correlation or covariance matrix.
 */
@NameCn("主成分分析")
public class PCA extends Trainer <PCA, PCAModel> implements
	PcaTrainParams <PCA>,
	PcaPredictParams <PCA>,
	HasLazyPrintModelInfo <PCA> {

	private static final long serialVersionUID = 7745465302322281797L;

	public PCA() {
		super();
	}

	public PCA(Params params) {
		super(params);
	}

}
