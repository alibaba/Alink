package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelInfo;

import java.util.List;

public class EqualWidthDiscretizerModelInfoBatchOp
	extends
	ExtractModelInfoBatchOp <EqualWidthDiscretizerModelInfoBatchOp.EqualWidthDiscretizerModelInfo,
		EqualWidthDiscretizerModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public EqualWidthDiscretizerModelInfoBatchOp() {
		this(null);
	}

	public EqualWidthDiscretizerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public EqualWidthDiscretizerModelInfo createModelInfo(List <Row> rows) {
		return new EqualWidthDiscretizerModelInfo(rows);
	}

	/**
	 * Summary of EqualWidth Discretizer Model;
	 */
	public static class EqualWidthDiscretizerModelInfo extends QuantileDiscretizerModelInfo {
		private static final long serialVersionUID = -4990829552802168917L;

		public EqualWidthDiscretizerModelInfo(List <Row> list) {
			super(list);
		}
	}
}
