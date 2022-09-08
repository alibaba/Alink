package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocCountVectorizerTrainBatchOp.
 * It supports several types: IDF/TF/TF-IDF/One-Hot/WordCount.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本特征生成预测")
public final class DocCountVectorizerPredictLocalOp extends ModelMapLocalOp <DocCountVectorizerPredictLocalOp>
	implements DocCountVectorizerPredictParams <DocCountVectorizerPredictLocalOp> {

	public DocCountVectorizerPredictLocalOp() {
		this(null);
	}

	public DocCountVectorizerPredictLocalOp(Params params) {
		super(DocCountVectorizerModelMapper::new, params);
	}
}
