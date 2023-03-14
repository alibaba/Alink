package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.MultiHotModelInfo;

import java.util.List;

/**
 * MultiHotModelInfoBatchOp can be linked to the output of MultiHotTrainBatchOp to summary the MultiHot model.
 */
public class MultiHotModelInfoBatchOp extends ExtractModelInfoBatchOp <MultiHotModelInfo, MultiHotModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public MultiHotModelInfoBatchOp() {
		this(null);
	}

	public MultiHotModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public MultiHotModelInfo createModelInfo(List <Row> rows) {
		return new MultiHotModelInfo(rows);
	}
}
