package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.featurebuilder.DateUtil;
import com.alibaba.alink.params.feature.featuregenerator.OverTimeWindowParams;

/**
 * Stream feature builder base on over window with user-defined recent several time of data.
 */
@NameCn("特征构造：OverTimeWindow")
public class OverTimeWindowStreamOp
	extends BaseOverWindowStreamOp <OverTimeWindowStreamOp>
	implements OverTimeWindowParams <OverTimeWindowStreamOp> {

	public OverTimeWindowStreamOp() {
		super(null);
	}

	public OverTimeWindowStreamOp(Params params) {
		super(params);
	}

	@Override
	public void generateWindowClause() {
		Double timeInterval = getPrecedingTime();
		String windowClause;
		if (timeInterval == null) {
			windowClause = "UNBOUNDED ";
			timeInterval = -1.0;
		} else {
			windowClause = "INTERVAL " + DateUtil.parseSecondTime(timeInterval);
		}
		setWindowParams("RANGE", windowClause, timeInterval);
	}
}
