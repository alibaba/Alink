package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.feature.ExclusiveFeatureBundleModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

/**
 * ExclusiveFeatureBundleModelInfoLocalOp can be linked to the output of ExclusiveFeatureBundleTrainLocalOp to summary the model.
 */
public class ExclusiveFeatureBundleModelInfoLocalOp extends ExtractModelInfoLocalOp <ExclusiveFeatureBundleModelInfo, ExclusiveFeatureBundleModelInfoLocalOp> {

	public ExclusiveFeatureBundleModelInfoLocalOp() {
		this(null);
	}

	public ExclusiveFeatureBundleModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	public ExclusiveFeatureBundleModelInfo createModelInfo(List <Row> rows) {
		return new ExclusiveFeatureBundleModelInfo(rows);
	}
}
