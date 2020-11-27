package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.recommendation.AlsModelInfo;

import java.util.List;

/**
 * Als model info.
 */
public class AlsModelInfoBatchOp extends ExtractModelInfoBatchOp <AlsModelInfo, AlsModelInfoBatchOp> {
	private static final long serialVersionUID = -744424427167310133L;

	public AlsModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected AlsModelInfo createModelInfo(List <Row> rows) {
		return new AlsModelInfo(0, 0, 0, getParams());
	}
}
