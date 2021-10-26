package com.alibaba.alink.server.service;

import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;

import java.util.List;

public interface ExperimentService {
	void checkExperimentId(Long experimentId);

	Node secureGetNode(Long experimentId, Long nodeId);

	Edge secureGetEdge(Long experimentId, Long edgeId);

	void runExperiment(Long experimentId) throws Exception;

	List <String> exportScripts(Long experimentId) throws ClassNotFoundException;
}
