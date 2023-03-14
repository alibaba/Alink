package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.ExclusiveFeatureBundleModelInfo;

import java.util.List;

/**
 * ExclusiveFeatureBundleModelInfoLocalOp can be linked to the output of ExclusiveFeatureBundleTrainLocalOp to summary the model.
 */
public class ExclusiveFeatureBundleModelInfoBatchOp
	extends ExtractModelInfoBatchOp <ExclusiveFeatureBundleModelInfo, ExclusiveFeatureBundleModelInfoBatchOp> {

	public ExclusiveFeatureBundleModelInfoBatchOp() {
		this(null);
	}

	public ExclusiveFeatureBundleModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public ExclusiveFeatureBundleModelInfo createModelInfo(List <Row> rows) {
		return new ExclusiveFeatureBundleModelInfo(rows);
	}
}
