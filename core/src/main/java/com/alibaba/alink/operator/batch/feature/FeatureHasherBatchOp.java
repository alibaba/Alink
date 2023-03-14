package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.params.feature.FeatureHasherParams;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 *
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "categoricalCols")
@NameCn("特征哈希")
@NameEn("Feature Hasher")
public final class FeatureHasherBatchOp extends MapBatchOp <FeatureHasherBatchOp>
	implements FeatureHasherParams <FeatureHasherBatchOp> {
	private static final long serialVersionUID = 6037792513321750824L;

	public FeatureHasherBatchOp() {
		this(null);
	}

	public FeatureHasherBatchOp(Params params) {
		super(FeatureHasherMapper::new, params);
	}
}
