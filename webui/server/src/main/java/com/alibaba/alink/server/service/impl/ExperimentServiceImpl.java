package com.alibaba.alink.server.service.impl;

import com.alibaba.alink.server.common.ExportScriptUtils;
import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Experiment;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import com.alibaba.alink.server.excpetion.InvalidEdgeIdException;
import com.alibaba.alink.server.excpetion.InvalidNodeIdException;
import com.alibaba.alink.server.repository.EdgeRepository;
import com.alibaba.alink.server.repository.ExperimentRepository;
import com.alibaba.alink.server.repository.NodeParamRepository;
import com.alibaba.alink.server.repository.NodeRepository;
import com.alibaba.alink.server.service.ExecutionService;
import com.alibaba.alink.server.service.ExperimentService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ExperimentServiceImpl implements ExperimentService {

	@Autowired
	NodeRepository nodeRepository;

	@Autowired
	EdgeRepository edgeRepository;

	@Autowired
	NodeParamRepository nodeParamRepository;

	@Autowired
	ExperimentRepository experimentRepository;

	@Autowired
	ExecutionService executionService;

	@Override
	public void checkExperimentId(Long experimentId) {
		if (experimentId == null) {
			throw new IllegalArgumentException("Experiment ID is null!");
		}
		if (!experimentId.equals(1L)) {
			throw new IllegalArgumentException("Experiment ID must be 1!");
		}
	}

	@Override
	public Node secureGetNode(Long experimentId, Long nodeId) {
		checkExperimentId(experimentId);
		Node node = nodeRepository.findById(nodeId).orElseThrow(() -> new InvalidNodeIdException(nodeId));
		if (!node.getExperimentId().equals(experimentId)) {
			throw new IllegalArgumentException(
				String.format("Node [%d] not belongs to Experiment [%d]!", nodeId, experimentId));
		}
		return node;
	}

	@Override
	public Edge secureGetEdge(Long experimentId, Long edgeId) {
		checkExperimentId(experimentId);
		Edge edge = edgeRepository.findById(edgeId).orElseThrow(() -> new InvalidEdgeIdException(edgeId));
		if (!edge.getExperimentId().equals(experimentId)) {
			throw new IllegalArgumentException(
				String.format("Edge [%d] not belongs to Experiment [%d]!", edgeId, experimentId));
		}
		return edge;
	}

	@Override
	public void runExperiment(Long experimentId) throws Exception {
		checkExperimentId(experimentId);
		List <Node> nodes = nodeRepository.findByExperimentId(experimentId);
		List <Edge> edges = edgeRepository.findByExperimentId(experimentId);
		List <NodeParam> nodeParams = nodeParamRepository.findByExperimentId(experimentId);
		Experiment experiment = experimentRepository.findById(experimentId).orElse(new Experiment());

		String configStr = experiment.getConfig();
		if (StringUtils.isBlank(configStr)) {
			configStr = "{}";
		}
		Map <String, String> config = new Gson().fromJson(configStr,
			new TypeToken <Map <String, String>>() {}.getType());
		executionService.run(nodes, edges, nodeParams, config);
	}

	@Override
	public List <String> exportScripts(Long experimentId) throws ClassNotFoundException {
		List <Node> nodes = nodeRepository.findByExperimentId(experimentId);
		List <Edge> edges = edgeRepository.findByExperimentId(experimentId);
		List <NodeParam> nodeParams = nodeParamRepository.findByExperimentId(experimentId);
		Experiment experiment = experimentRepository.findById(experimentId).orElse(new Experiment());

		String configStr = experiment.getConfig();
		if (StringUtils.isBlank(configStr)) {
			configStr = "{}";
		}
		Map <String, String> config = new Gson().fromJson(configStr,
			new TypeToken <Map <String, String>>() {}.getType());

		List <String> lines = executionService.getScriptHeader(config);
		lines.add("\n");
		lines.addAll(ExportScriptUtils.generateDAGScript(nodes, edges, nodeParams));
		return lines;
	}
}
