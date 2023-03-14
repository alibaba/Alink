package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.fm.FmClassifierModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

/**
 * Fm model info local op.
 */
public class FmClassifierModelInfoLocalOp
	extends ExtractModelInfoLocalOp <FmClassifierModelInfo, FmClassifierModelInfoLocalOp> {

	private static final long serialVersionUID = -3598963496955727614L;

	public FmClassifierModelInfoLocalOp() {
		this(null);
	}

	public FmClassifierModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected FmClassifierModelInfo createModelInfo(List <Row> rows) {
		return new FmClassifierModelInfo(rows);
	}

}
