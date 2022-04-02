package com.alibaba.alink.python.utils;

import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.TransformerBase;

@SuppressWarnings("unused")
public class OpTypeUtil {
	public static boolean isEstimatorBase(Object object) {
		return object instanceof EstimatorBase;
	}

	public static boolean isModelBase(Object object) {
		return object instanceof ModelBase;
	}

	public static boolean isTransformerBase(Object object) {
		return object instanceof TransformerBase;
	}
}
