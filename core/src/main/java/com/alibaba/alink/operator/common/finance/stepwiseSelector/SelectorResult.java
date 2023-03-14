package com.alibaba.alink.operator.common.finance.stepwiseSelector;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

public abstract class SelectorResult implements AlinkSerializable, Serializable {
	public int[] selectedIndices;
	public String[] selectedCols;

	public abstract String toVizData();
}
