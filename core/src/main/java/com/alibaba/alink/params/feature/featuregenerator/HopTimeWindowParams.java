package com.alibaba.alink.params.feature.featuregenerator;

public interface HopTimeWindowParams<T> extends
	GroupTimeWindowParams <T>,
	HasWindowTime <T>,
	HasHopTime <T>{
}
