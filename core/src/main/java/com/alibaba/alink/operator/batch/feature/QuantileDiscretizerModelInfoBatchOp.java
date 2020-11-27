package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelInfo;

import java.util.List;

/**
 * QuantileDiscretizerModelInfoBatchOp can be linked to the output of QuantileDiscretizerTrainBatchOp to summary the
 * QuantileDiscretizer model.
 */
public class QuantileDiscretizerModelInfoBatchOp
	extends ExtractModelInfoBatchOp <QuantileDiscretizerModelInfo, QuantileDiscretizerModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public QuantileDiscretizerModelInfoBatchOp() {
		this(null);
	}

	public QuantileDiscretizerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public QuantileDiscretizerModelInfo createModelInfo(List <Row> rows) {
		return new QuantileDiscretizerModelInfo(rows);
	}
}
