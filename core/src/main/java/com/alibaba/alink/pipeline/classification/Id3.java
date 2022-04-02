package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.Id3TrainBatchOp;
import com.alibaba.alink.params.classification.Id3PredictParams;
import com.alibaba.alink.params.classification.Id3TrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The pipeline for id3 model.
 */
@NameCn("ID3决策树分类")
public class Id3 extends Trainer <Id3, Id3Model> implements
	Id3TrainParams <Id3>,
	Id3PredictParams <Id3>, HasLazyPrintModelInfo <Id3> {

	private static final long serialVersionUID = 8046279617979726681L;

	public Id3() {
		super();
	}

	public Id3(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new Id3TrainBatchOp(this.getParams()).linkFrom(in);
	}

}
