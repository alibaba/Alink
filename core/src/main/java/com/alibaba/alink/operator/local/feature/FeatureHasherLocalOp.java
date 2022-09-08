package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.feature.FeatureHasherParams;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 * <p>
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "categoricalCols")
@NameCn("特征哈希")
public final class FeatureHasherLocalOp extends MapLocalOp <FeatureHasherLocalOp>
	implements FeatureHasherParams <FeatureHasherLocalOp> {
	private static final long serialVersionUID = 6037792513321750824L;

	public FeatureHasherLocalOp() {
		this(null);
	}

	public FeatureHasherLocalOp(Params params) {
		super(FeatureHasherMapper::new, params);
	}
}
