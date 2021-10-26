package com.alibaba.alink.server.controller;

import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Experiment;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.repository.EdgeRepository;
import com.alibaba.alink.server.repository.ExperimentRepository;
import com.alibaba.alink.server.repository.NodeParamRepository;
import com.alibaba.alink.server.repository.NodeRepository;
import com.alibaba.alink.server.service.ExperimentService;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RestController
public class ExperimentController {

	private final static Logger LOG = LoggerFactory.getLogger(ExperimentController.class);

	@Autowired
	ExperimentService experimentService;

	@Autowired
	NodeRepository nodeRepository;

	@Autowired
	EdgeRepository edgeRepository;

	@Autowired
	ExperimentRepository experimentRepository;

	@Autowired
	NodeParamRepository nodeParamRepository;

	/**
	 * Get content of an experiment
	 *
	 * @param experimentId Experiment ID
	 * @return
	 */
	@RequestMapping(
		value = "/experiment/get_graph",
		method = RequestMethod.GET,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public GetExperimentGraphResponse getExperimentContent(
		@RequestParam(value = "experiment_id", defaultValue = "1") Long experimentId) {
		experimentService.checkExperimentId(experimentId);
		List <Node> nodes = nodeRepository.findByExperimentId(experimentId);
		List <Edge> edges = edgeRepository.findByExperimentId(experimentId);

		Set <Long> nodeIdSet = new HashSet <>();
		for (Node node : nodes) {
			nodeIdSet.add(node.getId());
		}
		List <Edge> toDeleteEdges = new ArrayList <>();
		for (Edge edge : edges) {
			if (!nodeIdSet.contains(edge.getSrcNodeId()) || !nodeIdSet.contains(edge.getDstNodeId())) {
				toDeleteEdges.add(edge);
				LOG.info(String.format("About to delete invalid edge (%d.%d) -> (%d.%d)",
					edge.getSrcNodeId(), edge.getSrcNodePort(),
					edge.getDstNodeId(), edge.getDstNodePort())
				);
			}
		}

		if (toDeleteEdges.size() > 0) {
			edgeRepository.deleteAllInBatch(toDeleteEdges);
			edgeRepository.flush();
			edges = edgeRepository.findByExperimentId(experimentId);
		}

		return new GetExperimentGraphResponse(nodes, edges);
	}

	/**
	 * Export PyAlink script
	 *
	 * @param experimentId Experiment ID
	 * @return
	 */
	@RequestMapping(
		value = "/experiment/export_pyalink_script",
		method = RequestMethod.GET,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public ExportPyAlinkScriptResponse exportPyAlinkScript(
		@RequestParam(value = "experiment_id", defaultValue = "1") Long experimentId) throws ClassNotFoundException {
		experimentService.checkExperimentId(experimentId);
		List <String> lines = experimentService.exportScripts(experimentId);
		return new ExportPyAlinkScriptResponse(lines);
	}

	/**
	 * Run an experiment
	 *
	 * @param experimentId Experiment ID
	 * @return
	 */
	@RequestMapping(
		value = "/experiment/run",
		method = RequestMethod.GET,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public BasicResponse runExperiment(@RequestParam(value = "experiment_id", defaultValue = "1") Long experimentId)
		throws Exception {
		experimentService.runExperiment(experimentId);
		return BasicResponse.OK();
	}

	@RequestMapping(
		value = "/experiment/update",
		method = RequestMethod.POST,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public BasicResponse updateExperiment(@Valid @RequestBody ExperimentController.UpdateExperimentRequest request) {
		experimentService.checkExperimentId(request.id);
		Experiment experiment = experimentRepository.findById(request.id)
			.orElse(new Experiment().setName("unnamed"));
		if (null != request.name) {
			experiment.setName(request.name);
		}
		if (null != request.config) {
			experiment.setConfig(request.config);
		}
		experimentRepository.saveAndFlush(experiment);
		return BasicResponse.OK();
	}

	@RequestMapping(
		value = "/experiment/get",
		method = RequestMethod.GET,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public GetExperimentResponse getExperiment(@RequestParam(value = "experiment_id", defaultValue = "1") Long id) {
		experimentService.checkExperimentId(id);
		Experiment experiment = experimentRepository.findById(id)
			.orElse(new Experiment().setName("unnamed"));
		return new GetExperimentResponse(experiment);
	}

	public static class GetExperimentGraphResponse extends BasicResponse {
		public DataT data = new DataT();

		public GetExperimentGraphResponse(List <Node> nodes, List <Edge> edges) {
			super("OK");
			this.data.nodes = nodes;
			this.data.edges = edges;
		}

		static class DataT {
			/**
			 * Node list
			 */
			public List <Node> nodes;
			/**
			 * Edge list
			 */
			public List <Edge> edges;
		}
	}

	public static class ExportPyAlinkScriptResponse extends BasicResponse {
		public DataT data = new DataT();

		public ExportPyAlinkScriptResponse(List <String> lines) {
			super("OK");
			this.data.lines = lines;
		}

		static class DataT {
			/**
			 * Code lines
			 */
			public List <String> lines;
		}
	}

	public static class UpdateExperimentRequest {
		/**
		 * Experiment ID
		 */
		public Long id = 1L;
		/**
		 * Name
		 */
		public String name;
		/**
		 * Configuration, in JSON format
		 */
		public String config;
	}

	public static class GetExperimentResponse extends BasicResponse {
		public DataT data = new DataT();

		public GetExperimentResponse(Experiment experiment) {
			super("OK");
			this.data.experiment = experiment;
		}

		static class DataT {
			/**
			 * Experiment
			 */
			public Experiment experiment;
		}
	}
}
