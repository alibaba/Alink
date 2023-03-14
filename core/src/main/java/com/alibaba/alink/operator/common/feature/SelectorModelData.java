package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.utils.AlinkSerializable;

public class SelectorModelData implements AlinkSerializable {
	public int[] selectedIndices;
	public String vectorColName;
	public String[] vectorColNames;
}
