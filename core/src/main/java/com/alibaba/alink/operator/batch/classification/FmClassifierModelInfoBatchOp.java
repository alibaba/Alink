package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.FmClassifierModelInfo;

import java.util.List;

/**
 * FmModelInfoBatchOp can be linked to the output of BaseFmTrainBatchOp to summary the Fm model.
 */
public class FmClassifierModelInfoBatchOp
	extends ExtractModelInfoBatchOp <FmClassifierModelInfo, FmClassifierModelInfoBatchOp> {
	private static final long serialVersionUID = -1834964301108717272L;

	public FmClassifierModelInfoBatchOp() {
		this(new Params());
	}

	/**
	 * construct function.
	 *
	 * @param params Parameters of batchOp.
	 */
	public FmClassifierModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected FmClassifierModelInfo createModelInfo(List <Row> rows) {
		return new FmClassifierModelInfo(rows);
	}

	@Override
	protected BatchOperator <?> processModel() {
		return this;
	}
}
