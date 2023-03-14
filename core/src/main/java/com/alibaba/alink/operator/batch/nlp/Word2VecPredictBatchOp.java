package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.nlp.Word2VecModelMapper;
import com.alibaba.alink.params.nlp.Word2VecPredictParams;

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
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("Word2Vec预测")
@NameEn("Word2Vec Prediction")
public class Word2VecPredictBatchOp extends ModelMapBatchOp <Word2VecPredictBatchOp>
	implements Word2VecPredictParams <Word2VecPredictBatchOp> {
	private static final long serialVersionUID = 1415195739005424277L;

	public Word2VecPredictBatchOp() {
		this(null);
	}

	public Word2VecPredictBatchOp(Params params) {
		super(Word2VecModelMapper::new, params);
	}
}
