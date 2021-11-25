package com.alibaba.alink.common.io.plugin;

import java.util.Map;

@DistributeCacheGeneratorPolicy(policy = "plugin")
public class PluginDistributeCacheGenerator extends DistributeCacheGenerator {

	@Override
	public DistributeCache generate(Map <String, String> context) {
		return new PluginDistributeCache(context);
	}
}
