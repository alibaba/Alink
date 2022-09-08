package com.alibaba.alink.operator.batch.huge.word2vec;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.operator.common.aps.ApsFuncUpdateModel;

import java.util.List;

public class ApsFuncUpdateModelW2V extends ApsFuncUpdateModel <float[]> {
	private static final long serialVersionUID = -5359588902595016659L;

	@Override
	public float[] update(float[] oldFeaVal, List <float[]> newFeaVals) {
		return update1(oldFeaVal, newFeaVals);
	}

	public float[] update1(float[] oldFeaVal, List <float[]> newFeaVals) {
		if (null == newFeaVals || newFeaVals.size() < 1) {
			throw new AkUnclassifiedErrorException("ApsFunction meets RuntimeException");
		}

		float inverseCnt = 1.f / newFeaVals.size();
		int len = newFeaVals.get(0).length;
		float[] res = new float[len];

		for (float[] vec : newFeaVals) {
			for (int i = 0; i < len; i++) {
				res[i] += vec[i];
			}
		}

		for (int i = 0; i < len; i++) {
			res[i] *= inverseCnt;
		}

		return res;
	}
}
