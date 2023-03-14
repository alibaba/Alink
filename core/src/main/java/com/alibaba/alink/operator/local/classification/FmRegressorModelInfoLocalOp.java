package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.fm.FmRegressorModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

/**
 * Fm model info local op.
 */
public class FmRegressorModelInfoLocalOp
	extends ExtractModelInfoLocalOp <FmRegressorModelInfo, FmRegressorModelInfoLocalOp> {

	private static final long serialVersionUID = -3598963496955727614L;

	public FmRegressorModelInfoLocalOp() {
		this(null);
	}

	public FmRegressorModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected FmRegressorModelInfo createModelInfo(List <Row> rows) {
		return new FmRegressorModelInfo(rows);
	}

}
