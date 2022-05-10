package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.featurebuilder.BaseGroupTimeWindowStreamOp;
import com.alibaba.alink.operator.common.feature.featurebuilder.GroupWindowType;
import com.alibaba.alink.params.feature.featuregenerator.TumbleTimeWindowParams;

/**
 * Stream feature builder base on tumble group window.
 */
@NameCn("特征构造：滚动窗口")
public class TumbleTimeWindowStreamOp
	extends BaseGroupTimeWindowStreamOp <TumbleTimeWindowStreamOp>
	implements TumbleTimeWindowParams <TumbleTimeWindowStreamOp> {

	public TumbleTimeWindowStreamOp() {
		this(null);
	}

	public TumbleTimeWindowStreamOp(Params params) {
		super(params, GroupWindowType.TUMBLE);
	}
}
