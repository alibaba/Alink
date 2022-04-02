package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.io.plugin.PluginDescriptor;

import java.util.function.Function;

class DLPredictorVersionGetter
	implements Function <Tuple2 <DLPredictorService, PluginDescriptor>, String> {
	@Override
	public String apply(Tuple2 <DLPredictorService, PluginDescriptor> service) {
		return service.f1.getVersion();
	}
}
