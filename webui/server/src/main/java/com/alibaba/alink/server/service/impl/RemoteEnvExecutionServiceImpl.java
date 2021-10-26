package com.alibaba.alink.server.service.impl;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.server.configuration.ExecutionConfig;
import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import com.alibaba.alink.testutil.envfactory.impl.RemoteEnvFactoryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Service
@ConditionalOnProperty(name = "alink.execution.type", havingValue = "remote")
@Qualifier("removeEnv")
public class RemoteEnvExecutionServiceImpl extends EnvExecutionServiceImpl {

	@Autowired
	ExecutionConfig executionConfig;

	RemoteEnvFactoryImpl envFactory = new RemoteEnvFactoryImpl();

	@Override
	MLEnvironment getMLEnv() {
		return (MLEnvironment) envFactory.getMlEnv();
	}

	@Override
	public void run(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams, Map <String, String> config)
		throws Exception {
		Properties properties = new Properties();
		properties.setProperty("host", executionConfig.getRemoteClusterHost());
		properties.setProperty("port", executionConfig.getRemoteClusterPort());
		properties.setProperty("parallelism", config.getOrDefault("parallelism", "2"));
		envFactory.initialize(properties);

		runImpl(nodes, edges, nodeParams);
	}

	@Override
	public List <String> getScriptHeader(Map <String, String> config) {
		String parallelism = config.getOrDefault("parallelism", "2");
		List <String> lines = new ArrayList <>();
		lines.add("from pyalink.alink import *");
		lines.add(String.format("useRemoteEnv(host='%s', port=%s, parallelism=%s)",
			executionConfig.getRemoteClusterHost(),
			executionConfig.getRemoteClusterPort(),
			parallelism));
		return lines;
	}
}
