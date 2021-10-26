package com.alibaba.alink.server.service.impl;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import com.alibaba.alink.testutil.envfactory.impl.LocalEnvFactoryImpl;
import javax.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Service
@ConditionalOnProperty(name = "alink.execution.type", havingValue = "embed", matchIfMissing = true)
@Qualifier("localEnv")
public class EmbedExecutionServiceImpl extends EnvExecutionServiceImpl {

	LocalEnvFactoryImpl envFactory = new LocalEnvFactoryImpl();
	boolean initialized = false;
	String lastParallelism = null;

	@Override
	MLEnvironment getMLEnv() {
		return (MLEnvironment) envFactory.getMlEnv();
	}

	@PreDestroy
	public void destroy() {
		if (initialized) {
			envFactory.destroy();
		}
	}

	@Override
	public void run(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams, Map <String, String> config)
		throws Exception {
		String parallelism = config.getOrDefault("parallelism", "2");
		boolean reuse = parallelism.equals(lastParallelism);
		// TODO: may not work correctly when running multiple jobs simultaneously
		if (!reuse) {
			if (initialized) {
				envFactory.destroy();
			}
			Properties properties = new Properties();
			properties.setProperty("parallelism", parallelism);
			envFactory.initialize(properties);
			lastParallelism = parallelism;
		}
		initialized = true;
		runImpl(nodes, edges, nodeParams);
	}

	@Override
	public List <String> getScriptHeader(Map <String, String> config) {
		String parallelism = config.getOrDefault("parallelism", "2");
		List <String> lines = new ArrayList <>();
		lines.add("from pyalink.alink import *");
		lines.add(String.format("useLocalEnv(%s)", parallelism));
		return lines;
	}
}
