package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceNStatFeatures;

public class LatestNCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <LatestNCrossCategoricalStatFeatures>
	implements InterfaceNStatFeatures {
	public int[] numbers;

	public LatestNCrossCategoricalStatFeatures() {
		super();
	}

	public LatestNCrossCategoricalStatFeatures setNumbers(int... numbers) {
		this.numbers = numbers;
		return this;
	}

	@Override
	public int[] getNumbers() {
		return numbers;
	}

}
