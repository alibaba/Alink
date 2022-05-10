package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.featurebuilder.BaseGroupTimeWindowStreamOp;
import com.alibaba.alink.operator.common.feature.featurebuilder.BaseOverWindowStreamOp;
import com.alibaba.alink.operator.common.feature.featurebuilder.DateUtil;
import com.alibaba.alink.params.feature.featuregenerator.OverTimeWindowParams;
import org.apache.commons.lang3.math.NumberUtils;

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
		String timeIntervalWithUnit = getPrecedingTime();
		Double timeInterval = null;
		if (NumberUtils.isNumber(timeIntervalWithUnit)) {
			timeInterval = NumberUtils.createDouble(timeIntervalWithUnit);
		}

		if (timeInterval != null) {
			timeIntervalWithUnit = null;
		}

		String windowClause;
		if (timeInterval == null && timeIntervalWithUnit == null) {
			windowClause = "UNBOUNDED ";
			timeInterval = -1.0;
		} else if (timeInterval != null && timeIntervalWithUnit == null) {
			windowClause = "INTERVAL " + DateUtil.parseSecondTime(timeInterval);
		} else if (timeInterval == null && timeIntervalWithUnit != null) {
			windowClause = BaseGroupTimeWindowStreamOp.parseWindowTime(timeInterval, timeIntervalWithUnit);
		} else {
			throw new RuntimeException("timeInterval or timeIntervalWithUnit must be set.");
		}

		if (timeInterval == null) {
			timeInterval = (double) getIntervalBySecond(timeIntervalWithUnit);
		}
		setWindowParams("RANGE", windowClause, timeInterval);
	}

	public static long getIntervalBySecond(String windowTime) {
		return getIntervalByMS(windowTime) / 1000;
	}

	static long getIntervalByMS(String windowWithUnit) {
		if (NumberUtils.isNumber(windowWithUnit)) {
			return ((long) NumberUtils.createInteger(windowWithUnit)) * 1000;
		}
		String unit = windowWithUnit.substring(windowWithUnit.length() - 1);
		int ti = Integer.parseInt(windowWithUnit.substring(0, windowWithUnit.length() - 1));
		switch (unit) {
			case "s":
				return ti * 1000L;
			case "m":
				return ti * 60_000L;
			case "h":
				return ti * 3600_000L;
			case "d":
				return ti * 43200_000L;
			default:
				throw new RuntimeException("It is not support yet.");
		}
	}
}
