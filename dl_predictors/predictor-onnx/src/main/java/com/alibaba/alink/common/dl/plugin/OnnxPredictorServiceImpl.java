package com.alibaba.alink.common.dl.plugin;

public class OnnxPredictorServiceImpl extends BaseDLProcessPredictorService <OnnxJavaPredictor> {
	@Override
	public Class <OnnxJavaPredictor> getPredictorClass() {
		return OnnxJavaPredictor.class;
	}
}
