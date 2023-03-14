package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the inverse document frequency(idf) statistics provided by
 * DocHashCountVectorizerTrainBatchOp. It uses MurmurHash 3 to get the hash value of a word as the index.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本哈希特征生成预测")
@NameEn("Document hash count vectorizer prediction")
public class DocHashCountVectorizerPredictStreamOp extends ModelMapStreamOp <DocHashCountVectorizerPredictStreamOp>
	implements DocHashCountVectorizerPredictParams <DocHashCountVectorizerPredictStreamOp> {
	private static final long serialVersionUID = 2575430297675390769L;

	public DocHashCountVectorizerPredictStreamOp() {
		super(DocHashCountVectorizerModelMapper::new, new Params());
	}

	public DocHashCountVectorizerPredictStreamOp(Params params) {
		super(DocHashCountVectorizerModelMapper::new, params);
	}

	public DocHashCountVectorizerPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public DocHashCountVectorizerPredictStreamOp(BatchOperator model, Params params) {
		super(model, DocHashCountVectorizerModelMapper::new, params);
	}
}
