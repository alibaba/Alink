package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.clustering.LdaPredictParams;
import com.alibaba.alink.params.clustering.LdaTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
@NameCn("LDA")
public class Lda extends Trainer <Lda, LdaModel> implements
	LdaTrainParams <Lda>,
	LdaPredictParams <Lda>, HasLazyPrintModelInfo <Lda> {

	private static final long serialVersionUID = 3058711507574545630L;

	public Lda() {
		super();
	}

	public Lda(Params params) {
		super(params);
	}

}
