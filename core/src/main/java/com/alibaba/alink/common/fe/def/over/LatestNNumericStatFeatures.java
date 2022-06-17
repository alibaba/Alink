package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceNStatFeatures;

public class LatestNNumericStatFeatures extends BaseNumericStatFeatures <LatestNNumericStatFeatures>
	implements InterfaceNStatFeatures {
	public int[] numbers;

	public LatestNNumericStatFeatures() {
		super();
	}

	public LatestNNumericStatFeatures setNumbers(int... numbers) {
		this.numbers = numbers;
		return this;
	}

	@Override
	public int[] getNumbers() {
		return numbers;
	}
}
