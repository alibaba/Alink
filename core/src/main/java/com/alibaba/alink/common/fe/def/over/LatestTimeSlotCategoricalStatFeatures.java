package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTimeSlotStatFeatures;

public class LatestTimeSlotCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <LatestTimeSlotCategoricalStatFeatures>
	implements InterfaceTimeSlotStatFeatures {
	public String[] timeSlots;

	public LatestTimeSlotCategoricalStatFeatures() {
		super();
	}

	public LatestTimeSlotCategoricalStatFeatures setTimeSlots(String... timeSlots) {
		this.timeSlots = timeSlots;
		return this;
	}

	@Override
	public String[] getTimeSlots() {
		return timeSlots;
	}
}
