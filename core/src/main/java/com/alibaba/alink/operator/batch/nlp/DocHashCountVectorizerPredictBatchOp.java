package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelMapper;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocHashCountVectorizerTrainBatchOp.
 * It uses MurmurHash 3 to get the hash value of a word as the index.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本哈希特征生成预测")
public class DocHashCountVectorizerPredictBatchOp extends ModelMapBatchOp <DocHashCountVectorizerPredictBatchOp>
	implements DocHashCountVectorizerPredictParams <DocHashCountVectorizerPredictBatchOp> {
	private static final long serialVersionUID = -6029385456358959482L;

	public DocHashCountVectorizerPredictBatchOp() {
		this(new Params());
	}

	public DocHashCountVectorizerPredictBatchOp(Params params) {
		super(DocHashCountVectorizerModelMapper::new, params);
	}
}
