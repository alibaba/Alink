package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTimeIntervalStatFeatures;

public class LatestTimeIntervalNumericStatFeatures
	extends BaseNumericStatFeatures <LatestTimeIntervalNumericStatFeatures>
	implements InterfaceTimeIntervalStatFeatures {
	public String[] timeIntervals;

	public LatestTimeIntervalNumericStatFeatures() {
		super();
	}

	public LatestTimeIntervalNumericStatFeatures setTimeIntervals(String... timeIntervals) {
		this.timeIntervals = timeIntervals;
		return this;
	}

	@Override
	public String[] getTimeIntervals() {
		return timeIntervals;
	}
}
