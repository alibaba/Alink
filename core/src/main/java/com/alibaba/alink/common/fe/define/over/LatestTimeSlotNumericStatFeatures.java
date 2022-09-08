package com.alibaba.alink.common.fe.define.over;

import com.alibaba.alink.common.fe.define.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTimeSlotStatFeatures;

public class LatestTimeSlotNumericStatFeatures extends BaseNumericStatFeatures <LatestTimeSlotNumericStatFeatures>
	implements InterfaceTimeSlotStatFeatures {
	public String[] timeSlots;

	public LatestTimeSlotNumericStatFeatures() {
		super();
	}

	public LatestTimeSlotNumericStatFeatures setTimeSlots(String... timeSlots) {
		this.timeSlots = timeSlots;
		return this;
	}

	@Override
	public String[] getTimeSlots() {
		return timeSlots;
	}
}
