package com.alibaba.alink.common.fe.define.over;

import com.alibaba.alink.common.fe.define.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTimeSlotStatFeatures;

public class LatestTimeSlotCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <LatestTimeSlotCrossCategoricalStatFeatures>
	implements InterfaceTimeSlotStatFeatures {
	public String[] timeSlots;

	public LatestTimeSlotCrossCategoricalStatFeatures() {
		super();
	}

	public LatestTimeSlotCrossCategoricalStatFeatures setTimeSlots(String... timeSlots) {
		this.timeSlots = timeSlots;
		return this;
	}

	@Override
	public String[] getTimeSlots() {
		return timeSlots;
	}
}
