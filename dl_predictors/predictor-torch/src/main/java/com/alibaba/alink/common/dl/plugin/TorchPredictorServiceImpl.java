package com.alibaba.alink.common.dl.plugin;

public class TorchPredictorServiceImpl extends BaseDLProcessPredictorService <TorchJavaPredictor> {
	@Override
	public Class <TorchJavaPredictor> getPredictorClass() {
		return TorchJavaPredictor.class;
	}
}
