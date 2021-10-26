package com.alibaba.alink.server.service;

import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface ExecutionService {

	default void run(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams) throws Exception {
		run(nodes, edges, nodeParams, Collections.emptyMap());
	}

	void run(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams, Map <String, String> config)
		throws Exception;

	List <String> getScriptHeader(Map <String, String> config);
}
