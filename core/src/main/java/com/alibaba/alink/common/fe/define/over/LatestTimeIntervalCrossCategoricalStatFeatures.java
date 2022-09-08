package com.alibaba.alink.common.fe.define.over;

import com.alibaba.alink.common.fe.define.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTimeIntervalStatFeatures;

public class LatestTimeIntervalCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <LatestTimeIntervalCrossCategoricalStatFeatures>
	implements InterfaceTimeIntervalStatFeatures {
	public String[] timeIntervals;

	public LatestTimeIntervalCrossCategoricalStatFeatures() {
		super();
	}

	public LatestTimeIntervalCrossCategoricalStatFeatures setTimeIntervals(String... timeIntervals) {
		this.timeIntervals = timeIntervals;
		return this;
	}

	@Override
	public String[] getTimeIntervals() {
		return timeIntervals;
	}
}
