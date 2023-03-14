package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.pca.PcaModelData;
import com.alibaba.alink.operator.common.feature.pca.PcaModelDataConverter;

import java.util.List;

public class PcaModelInfoBatchOp
	extends ExtractModelInfoBatchOp <PcaModelData, PcaModelInfoBatchOp> {

	private static final long serialVersionUID = 3198740187166728990L;

	public PcaModelInfoBatchOp() {
		this(null);
	}

	public PcaModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected PcaModelData createModelInfo(List <Row> rows) {
		return new PcaModelDataConverter().load(rows);
	}

}
