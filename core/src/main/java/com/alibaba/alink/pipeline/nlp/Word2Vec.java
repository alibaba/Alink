package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.params.nlp.Word2VecPredictParams;
import com.alibaba.alink.params.nlp.Word2VecTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Word2vec is a group of related models that are used to produce word embeddings.
 * These models are shallow, two-layer neural networks that are trained to reconstruct
 * linguistic contexts of words.
 *
 * <p>reference:
 * <p>https://en.wikipedia.org/wiki/Word2vec
 * <p>Mikolov, Tomas; et al. (2013). "Efficient Estimation of Word Representations in Vector Space"
 * <p>Mikolov, Tomas; Sutskever, Ilya; Chen, Kai; Corrado, Greg S.; Dean, Jeff (2013).
 * Distributed representations of words and phrases and their compositionality.
 * <p>https://code.google.com/archive/p/word2vec/
 */
@NameCn("Word2Vec")
public class Word2Vec extends Trainer <Word2Vec, Word2VecModel>
	implements Word2VecTrainParams <Word2Vec>, Word2VecPredictParams <Word2Vec>, HasLazyPrintTrainInfo <Word2Vec> {
	private static final long serialVersionUID = -91548532293731389L;

	public Word2Vec() {
	}

	public Word2Vec(Params params) {
		super(params);
	}

}
