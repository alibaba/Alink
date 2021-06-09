package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.feature.featuregenerator.TumbleTimeWindowParams;

/**
 * Stream feature builder base on tumble group window.
 */
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
