package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.params.recommendation.FmPredictParams;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Fm classifier pipeline op.
 */
@NameCn("FM分类")
public class FmClassifier extends Trainer <FmClassifier, FmClassificationModel>
	implements FmTrainParams <FmClassifier>, FmPredictParams <FmClassifier>, HasLazyPrintModelInfo <FmClassifier>,
	HasLazyPrintTrainInfo <FmClassifier> {

	private static final long serialVersionUID = 1557009335800161587L;

	public FmClassifier() {
		super();
	}

	public FmClassifier(Params params) {
		super(params);
	}

}
