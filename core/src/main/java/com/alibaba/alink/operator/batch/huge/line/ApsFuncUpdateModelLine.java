package com.alibaba.alink.operator.batch.huge.line;

import com.alibaba.alink.operator.common.aps.ApsFuncUpdateModel;

import java.util.List;

public class ApsFuncUpdateModelLine extends ApsFuncUpdateModel <float[][]> {
	private static final long serialVersionUID = 2667414736766276193L;

	@Override
	public float[][] update(float[][] oldFeaVal, List <float[][]> newFeaVals) {
		int order = oldFeaVal.length;
		for (float[][] val : newFeaVals) {
			for (int i = 0; i < order; i++) {
				LinePullAndTrainOperation.floatAxpy(1, val[i], oldFeaVal[i]);
			}
		}
		return oldFeaVal;
	}
}
