package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationFlatModelMapper;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelMapper;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@Internal
@NameCn("TF表模型分类模型")
public class TFTableModelClassificationModel<T extends TFTableModelClassificationModel <T>> extends MapModel <T>
	implements TFTableModelClassificationPredictParams <T> {

	public TFTableModelClassificationModel() {this(null);}

	public TFTableModelClassificationModel(Params params) {
		super(TFTableModelClassificationModelMapper::new, params);
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(new FlatModelMapBatchOp <>(
			TFTableModelClassificationFlatModelMapper::new,
			params
		).linkFrom(this.getModelData(), input));
	}
}
