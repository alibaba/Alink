package com.alibaba.alink.server.controller;

import com.alibaba.alink.server.domain.NodeParam;
import com.alibaba.alink.server.repository.NodeParamRepository;
import com.alibaba.alink.server.service.ExperimentService;
import javax.transaction.Transactional;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@RestController
public class NodeParamController {

	@Autowired
	ExperimentService experimentService;

	@Autowired
	NodeParamRepository nodeParamRepository;

	/**
	 * Update parameters of a node
	 *
	 * @param request
	 * @return
	 */
	@RequestMapping(
		value = "/param/update",
		method = RequestMethod.POST,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Transactional
	public BasicResponse updateParam(@Valid @RequestBody NodeParamController.UpdateParamRequest request) {
		experimentService.secureGetNode(request.experimentId, request.nodeId);
		List <NodeParam> nodeParamList = nodeParamRepository
			.findByExperimentIdAndNodeId(request.experimentId, request.nodeId);

		Map <String, NodeParam> key2NodeParam = new HashMap <>();
		for (NodeParam nodeParam : nodeParamList) {
			key2NodeParam.put(nodeParam.getKey(), nodeParam);
		}

		HashSet <Long> toRemoveIdSet = new HashSet <>();
		for (NodeParam nodeParam : nodeParamList) {
			String key = nodeParam.getKey();
			if (request.paramsToDel.contains(key)) {
				toRemoveIdSet.add(nodeParam.getId());
			}
		}

		List <NodeParam> toUpdateList = new ArrayList <>();
		for (Entry <String, String> entry : request.paramsToUpdate.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			if (value == null || StringUtils.isEmpty(value)) {
				// Note that `value` should be after jsonized.
				// So if `value` is an empty string, we treat `key` as "not set" and remove it.
				if (key2NodeParam.containsKey(key)) {
					NodeParam nodeParam = key2NodeParam.get(key);
					toRemoveIdSet.add(nodeParam.getId());
				}
				continue;
			}
			NodeParam nodeParam;
			if (key2NodeParam.containsKey(key)) {
				nodeParam = key2NodeParam.get(key);
				toRemoveIdSet.remove(nodeParam.getId());
			} else {
				nodeParam = new NodeParam();
				nodeParam.setExperimentId(request.experimentId);
				nodeParam.setNodeId(request.nodeId);
				nodeParam.setKey(key);
			}
			nodeParam.setValue(value);
			toUpdateList.add(nodeParam);
		}

		nodeParamRepository.saveAllAndFlush(toUpdateList);
		nodeParamRepository.deleteAllById(toRemoveIdSet);

		return BasicResponse.OK();
	}

	/**
	 * Get parameters of a node
	 *
	 * @param experimentId experiment ID
	 * @param nodeId       node ID
	 * @return
	 */
	@RequestMapping(
		value = "/param/get",
		method = RequestMethod.GET,
		produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	public GetNodeParamRespT getNodeParam(@RequestParam(value = "experiment_id", defaultValue = "1") Long experimentId,
										  @RequestParam("node_id") Long nodeId) {
		experimentService.secureGetNode(experimentId, nodeId);
		List <NodeParam> nodeParams = nodeParamRepository.findByExperimentIdAndNodeId(experimentId, nodeId);
		return new GetNodeParamRespT(nodeParams);
	}

	static class UpdateParamRequest {
		/**
		 * Experiment ID
		 */
		public Long experimentId = 1L;
		/**
		 * Node ID
		 */
		@NotNull
		public Long nodeId;

		/**
		 * Node parameters need to update
		 */
		@NotNull
		public Map <String, String> paramsToUpdate;

		/**
		 * Node parameters to delete
		 */
		@NotNull
		public Set <String> paramsToDel;
	}

	static class GetNodeParamRespT extends BasicResponse {
		public DataT data = new DataT();

		GetNodeParamRespT(List <NodeParam> L) {
			super("OK");
			this.data.parameters = L;
		}

		static class DataT {
			/**
			 * parameter list
			 */
			public List <NodeParam> parameters;
		}
	}
}
