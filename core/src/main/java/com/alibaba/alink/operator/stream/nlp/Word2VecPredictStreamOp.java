package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import com.alibaba.alink.operator.common.nlp.Word2VecModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
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
public class Word2VecPredictStreamOp extends ModelMapStreamOp <Word2VecPredictStreamOp>
	implements Word2VecPredictParams <Word2VecPredictStreamOp> {
	private static final long serialVersionUID = 329339396597412614L;

	public Word2VecPredictStreamOp() {
		super(Word2VecModelMapper::new, new Params());
	}

	public Word2VecPredictStreamOp(Params params) {
		super(Word2VecModelMapper::new, params);
	}

	public Word2VecPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public Word2VecPredictStreamOp(BatchOperator model, Params params) {
		super(model, Word2VecModelMapper::new, params);
	}
}
