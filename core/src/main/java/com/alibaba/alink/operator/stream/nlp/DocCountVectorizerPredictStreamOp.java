package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocCountVectorizerTrainBatchOp.
 * It supports several types: IDF/TF/TF-IDF/One-Hot/WordCount.
 * It processes streaming data.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本特征生成预测")
public final class DocCountVectorizerPredictStreamOp extends ModelMapStreamOp <DocCountVectorizerPredictStreamOp>
	implements DocCountVectorizerPredictParams <DocCountVectorizerPredictStreamOp> {
	private static final long serialVersionUID = -3395777426757947565L;

	public DocCountVectorizerPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public DocCountVectorizerPredictStreamOp(BatchOperator model, Params params) {
		super(model, DocCountVectorizerModelMapper::new, params);
	}
}
