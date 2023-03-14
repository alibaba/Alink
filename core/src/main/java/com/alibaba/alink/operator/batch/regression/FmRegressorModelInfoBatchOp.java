package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.fm.FmRegressorModelInfo;

import java.util.List;

/**
 * FmModelInfoBatchOp can be linked to the output of BaseFmTrainBatchOp to summary the Fm model.
 */
public class FmRegressorModelInfoBatchOp
	extends ExtractModelInfoBatchOp <FmRegressorModelInfo, FmRegressorModelInfoBatchOp> {
	private static final long serialVersionUID = 7904342526890917457L;

	public FmRegressorModelInfoBatchOp() {
		this(new Params());
	}

	/**
	 * construct function.
	 *
	 * @param params
	 */
	public FmRegressorModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected FmRegressorModelInfo createModelInfo(List <Row> rows) {
		return new FmRegressorModelInfo(rows);
	}

}
