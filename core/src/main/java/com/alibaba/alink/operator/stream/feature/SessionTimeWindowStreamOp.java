package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.feature.featuregenerator.SessionTimeWindowParams;

/**
 * Stream feature builder base on session group window.
 */
public class SessionTimeWindowStreamOp
	extends BaseGroupTimeWindowStreamOp <SessionTimeWindowStreamOp>
	implements SessionTimeWindowParams <SessionTimeWindowStreamOp> {

	public SessionTimeWindowStreamOp() {
		this(null);
	}

	public SessionTimeWindowStreamOp(Params params) {
		super(params, GroupWindowType.SESSION);
	}
}
