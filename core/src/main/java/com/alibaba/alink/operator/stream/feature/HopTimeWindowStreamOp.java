package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.featurebuilder.BaseGroupTimeWindowStreamOp;
import com.alibaba.alink.operator.common.feature.featurebuilder.GroupWindowType;
import com.alibaba.alink.params.feature.featuregenerator.HopTimeWindowParams;

/**
 * Stream feature builder base on hop group window.
 */
@NameCn("特征构造：滑动窗口")
@NameEn("Hop time window generator")
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
