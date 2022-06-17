package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTimeIntervalStatFeatures;

public class LatestTimeIntervalCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <LatestTimeIntervalCategoricalStatFeatures>
	implements InterfaceTimeIntervalStatFeatures {
	public String[] timeIntervals;

	public LatestTimeIntervalCategoricalStatFeatures() {
		super();
	}

	public LatestTimeIntervalCategoricalStatFeatures setTimeIntervals(String... timeIntervals) {
		this.timeIntervals = timeIntervals;
		return this;
	}

	@Override
	public String[] getTimeIntervals() {
		return timeIntervals;
	}
}
