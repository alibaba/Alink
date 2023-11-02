package com.alibaba.alink.operator.common.finance.group;

import java.io.Serializable;

public class SplitInfo implements Serializable {
	public int splitColIdx;
	public boolean isStr;
	public int isNull;
	public int isElse;
	public int splitValueIdx;
	public double gain;
	public boolean calculated;

	public long leftDataCount;

	public long rightDataCount;

	public double[] leftWeights;

	public double[] rightWeights;

	public SplitInfo clone() {
		SplitInfo info = new SplitInfo();
		info.splitColIdx = splitColIdx;
		info.isStr = isStr;
		info.isElse = isElse;
		info.isNull = isNull;
		info.splitValueIdx = splitValueIdx;
		info.gain = gain;
		info.calculated = calculated;
		info.leftDataCount = leftDataCount;
		info.rightDataCount = rightDataCount;
		if (leftWeights != null) {
			info.leftWeights = leftWeights.clone();
		}
		if (rightWeights != null) {
			info.rightWeights = rightWeights.clone();
		}
		return info;
	}
}
