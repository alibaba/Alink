package com.alibaba.alink.common.fe.define.over;

import com.alibaba.alink.common.fe.define.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTimeIntervalStatFeatures;

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
