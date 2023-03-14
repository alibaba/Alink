package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.FeatureHasherParams;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 * <p>
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "categoricalCols")
@NameCn("特征哈希")
@NameEn("Feature hasher")
public class FeatureHasherStreamOp extends MapStreamOp <FeatureHasherStreamOp>
	implements FeatureHasherParams <FeatureHasherStreamOp> {
	private static final long serialVersionUID = -6769551866730160917L;

	public FeatureHasherStreamOp() {
		this(null);
	}

	public FeatureHasherStreamOp(Params params) {
		super(FeatureHasherMapper::new, params);
	}
}
