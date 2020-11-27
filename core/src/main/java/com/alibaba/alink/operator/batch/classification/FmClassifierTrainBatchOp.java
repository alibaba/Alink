package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.FmClassifierModelInfo;
import com.alibaba.alink.operator.common.fm.FmClassifierModelTrainInfo;
import com.alibaba.alink.operator.common.fm.FmTrainBatchOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import java.util.List;

/**
 * Fm classification train algorithm. the input of this algorithm can be vector or table.
 */
public class FmClassifierTrainBatchOp extends FmTrainBatchOp <FmClassifierTrainBatchOp>
	implements FmTrainParams <FmClassifierTrainBatchOp>,
	WithModelInfoBatchOp <FmClassifierModelInfo, FmClassifierTrainBatchOp, FmClassifierModelInfoBatchOp>,
	WithTrainInfo <FmClassifierModelTrainInfo, FmClassifierTrainBatchOp> {

	private static final long serialVersionUID = -8385944325790904485L;

	public FmClassifierTrainBatchOp() {
		super(new Params(), "binary_classification");
	}

	public FmClassifierTrainBatchOp(Params params) {
		super(params, "binary_classification");
	}

	@Override
	public FmClassifierModelTrainInfo createTrainInfo(List <Row> rows) {
		return new FmClassifierModelTrainInfo(rows);
	}

	@Override
	public BatchOperator <?> getSideOutputTrainInfo() {
		return this.getSideOutput(0);
	}

	/**
	 * get model info of this train process.
	 *
	 * @return
	 */
	@Override
	public FmClassifierModelInfoBatchOp getModelInfoBatchOp() {
		return new FmClassifierModelInfoBatchOp(this.labelType)
			.setMLEnvironmentId(this.getMLEnvironmentId()).linkFrom(this);
	}
}
