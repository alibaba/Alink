package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.OneHotModelInfo;

import java.util.List;

/**
 * OneHotModelInfoBatchOp can be linked to the output of OneHotTrainBatchOp to summary the OneHot model.
 */
public class OneHotModelInfoBatchOp extends ExtractModelInfoBatchOp <OneHotModelInfo, OneHotModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public OneHotModelInfoBatchOp() {
		this(null);
	}

	public OneHotModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public OneHotModelInfo createModelInfo(List <Row> rows) {
		return new OneHotModelInfo(rows);
	}
}
