package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.featuregenerator.HopTimeWindowParams;

/**
 * Stream feature builder base on hop group window.
 */
@NameCn("特征构造：滑动窗口")
public class HopTimeWindowStreamOp
	extends BaseGroupTimeWindowStreamOp <HopTimeWindowStreamOp>
	implements HopTimeWindowParams <HopTimeWindowStreamOp> {

	public HopTimeWindowStreamOp() {
		this(null);
	}

	public HopTimeWindowStreamOp(Params params) {
		super(params, GroupWindowType.HOP);
	}
}
