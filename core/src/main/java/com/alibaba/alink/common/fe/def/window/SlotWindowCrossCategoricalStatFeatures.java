package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceHopWindowStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceSlotWindowStatFeatures;

public class SlotWindowCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <SlotWindowCrossCategoricalStatFeatures>
	implements InterfaceSlotWindowStatFeatures {
	public String[] windowTimes;
	public String[] stepTimes;

	public SlotWindowCrossCategoricalStatFeatures() {
		super();
	}

	public SlotWindowCrossCategoricalStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	public SlotWindowCrossCategoricalStatFeatures setStepTimes(String... stepTimes) {
		this.stepTimes = stepTimes;
		return this;
	}

	@Override
	public String[] getWindowTimes() {
		return windowTimes;
	}

	@Override
	public String[] getStepTimes() {
		return stepTimes;
	}
}
