package com.alibaba.alink.server.controller;

import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.repository.EdgeRepository;
import com.alibaba.alink.server.service.ExperimentService;
import javax.transaction.Transactional;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EdgeController {

	@Autowired
	ExperimentService experimentService;

	@Autowired
	EdgeRepository edgeRepository;

	/**
	 * Add an edge
	 *
	 * @param request
	 * @return
	 */
	@RequestMapping(
		value = "/edge/add",
		method = RequestMethod.POST,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public AddEdgeResponse addEdge(@Valid @RequestBody AddEdgeRequest request) {
		experimentService.checkExperimentId(request.experimentId);
		Edge edge = new Edge();
		edge.setExperimentId(request.experimentId);
		edge.setSrcNodeId(request.srcNodeId);
		edge.setSrcNodePort(request.srcNodePort);
		edge.setDstNodeId(request.dstNodeId);
		edge.setDstNodePort(request.dstNodePort);
		edge = edgeRepository.saveAndFlush(edge);
		return new AddEdgeResponse(edge.getId());
	}

	/**
	 * Delete an edge
	 *
	 * @param experimentId Experiment ID
	 * @param edgeId       Edge ID
	 * @return
	 */
	@RequestMapping(
		value = "/edge/del",
		method = RequestMethod.GET,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public BasicResponse deleteEdge(@RequestParam(value = "experiment_id", defaultValue = "1") Long experimentId,
									@RequestParam("edge_id") Long edgeId) {
		Edge edge = experimentService.secureGetEdge(experimentId, edgeId);
		edgeRepository.delete(edge);
		return BasicResponse.OK();
	}

	/**
	 * Request for add an edge
	 */
	static class AddEdgeRequest {
		/**
		 * Experiment ID
		 */
		public Long experimentId = 1L;

		/**
		 * Source node ID
		 */
		@NotNull
		public Long srcNodeId;

		/**
		 * Source node port
		 */
		@NotNull
		public Short srcNodePort;

		/**
		 * Destination node ID
		 */
		@NotNull
		public Long dstNodeId;

		/**
		 * Destination node port
		 */
		@NotNull
		public Short dstNodePort;
	}

	static class AddEdgeResponse extends BasicResponse {
		/**
		 * data
		 */
		public DataT data = new DataT();

		public AddEdgeResponse(Long id) {
			super("OK");
			this.data.id = id;
		}

		static class DataT {
			/**
			 * Edge ID
			 */
			public Long id;
		}
	}
}
