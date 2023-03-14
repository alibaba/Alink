package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.classification.C45PredictParams;
import com.alibaba.alink.params.classification.C45TrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The pipeline for c45 model.
 */
@NameCn("C45决策树分类")
public class C45 extends Trainer <C45, C45Model> implements
	C45TrainParams <C45>,
	C45PredictParams <C45>, HasLazyPrintModelInfo <C45> {

	private static final long serialVersionUID = -2617518725119778276L;

	public C45() {
		super();
	}

	public C45(Params params) {
		super(params);
	}

}
