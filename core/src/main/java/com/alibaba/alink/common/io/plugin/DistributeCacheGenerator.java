package com.alibaba.alink.common.io.plugin;

import java.util.Map;

public abstract class DistributeCacheGenerator {

	public abstract DistributeCache generate(Map <String, String> context);

}
