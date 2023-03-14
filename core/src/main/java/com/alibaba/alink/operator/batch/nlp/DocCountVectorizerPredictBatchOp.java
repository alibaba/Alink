package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocCountVectorizerTrainBatchOp.
 * It supports several types: IDF/TF/TF-IDF/One-Hot/WordCount.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本特征生成预测")
@NameEn("Doc Count Vectorizer Prediction")
public final class DocCountVectorizerPredictBatchOp extends ModelMapBatchOp <DocCountVectorizerPredictBatchOp>
	implements DocCountVectorizerPredictParams <DocCountVectorizerPredictBatchOp> {
	private static final long serialVersionUID = 2584222216856311012L;

	public DocCountVectorizerPredictBatchOp() {
		this(null);
	}

	public DocCountVectorizerPredictBatchOp(Params params) {
		super(DocCountVectorizerModelMapper::new, params);
	}
}
