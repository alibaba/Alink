package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTimeIntervalStatFeatures;

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
