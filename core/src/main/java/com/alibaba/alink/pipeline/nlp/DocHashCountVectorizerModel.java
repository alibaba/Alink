package com.alibaba.alink.pipeline.nlp;

import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * DocCountVectorizerModel saves the document frequency, word count and inverse document
 * frequency of every word in the dataset.
 */
public class DocHashCountVectorizerModel extends MapModel<DocHashCountVectorizerModel>
	implements DocHashCountVectorizerPredictParams<DocHashCountVectorizerModel> {

	public DocHashCountVectorizerModel() {this(null);}

	public DocHashCountVectorizerModel(Params params) {
		super(DocHashCountVectorizerModelMapper::new, params);
	}

}
