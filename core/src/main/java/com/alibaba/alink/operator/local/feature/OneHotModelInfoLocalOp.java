package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.feature.OneHotModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

/**
 * OneHotModelInfoBatchOp can be linked to the output of OneHotTrainBatchOp to summary the OneHot model.
 */
public class OneHotModelInfoLocalOp extends ExtractModelInfoLocalOp <OneHotModelInfo, OneHotModelInfoLocalOp> {

	public OneHotModelInfoLocalOp() {
		this(null);
	}

	public OneHotModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	public OneHotModelInfo createModelInfo(List <Row> rows) {
		return new OneHotModelInfo(rows);
	}
}
