package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.featurebuilder.BaseGroupTimeWindowStreamOp;
import com.alibaba.alink.operator.common.feature.featurebuilder.GroupWindowType;
import com.alibaba.alink.params.feature.featuregenerator.SessionTimeWindowParams;

/**
 * Stream feature builder base on session group window.
 */
@NameCn("特征构造：会话窗口")
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
