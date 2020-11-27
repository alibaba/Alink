package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * DocHashIDFVectorizer converts a document to a sparse vector based on the inverse document frequency of every word in
 * the document.
 * Different from DocCountVectorizer, it used the hash value of the word as index.
 */
public class DocHashCountVectorizer extends Trainer <DocHashCountVectorizer, DocHashCountVectorizerModel>
	implements DocHashCountVectorizerPredictParams <DocHashCountVectorizer>,
	DocHashCountVectorizerTrainParams <DocHashCountVectorizer> {

	private static final long serialVersionUID = 5518784600471233227L;

	public DocHashCountVectorizer() {
		super();
	}

	public DocHashCountVectorizer(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new DocHashCountVectorizerTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
