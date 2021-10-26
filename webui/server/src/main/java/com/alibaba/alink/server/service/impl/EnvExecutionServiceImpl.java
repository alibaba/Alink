package com.alibaba.alink.server.service.impl;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.server.common.OpUtils;
import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import com.alibaba.alink.server.service.ExecutionService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Execute using ExecutionEnvironment
 */
public abstract class EnvExecutionServiceImpl implements ExecutionService {

	abstract MLEnvironment getMLEnv();

	public void runImpl(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams) throws Exception {
		MLEnvironment mlEnv = getMLEnv();
		Long mlEnvId = MLEnvironmentFactory.registerMLEnvironment(mlEnv);

		Map <Long, Set <NodeParam>> nodeParamMap = new HashMap <>();
		for (NodeParam nodeParam : nodeParams) {
			Long nodeId = nodeParam.getNodeId();
			if (!nodeParamMap.containsKey(nodeId)) {
				nodeParamMap.put(nodeId, new HashSet <>());
			}
			nodeParamMap.get(nodeId).add(nodeParam);
		}

		nodes.sort(Comparator.comparing(Node::getId));
		List <LinkedList <Tuple2 <Integer, Integer>>> inAdj = new ArrayList <>();
		List <LinkedList <Integer>> outAdj = new ArrayList <>();
		int v = nodes.size();
		int[] order = new int[v];

		for (int i = 0; i < v; ++i) {
			inAdj.add(new LinkedList <>());
			outAdj.add(new LinkedList <>());
			order[i] = 0;
		}

		int[] inDegree = new int[v];
		for (Edge edge : edges) {
			int src = Collections.binarySearch(
				nodes, new Node().setId(edge.getSrcNodeId()), Comparator.comparing(Node::getId)
			);
			int dst = Collections.binarySearch(
				nodes, new Node().setId(edge.getDstNodeId()), Comparator.comparing(Node::getId)
			);
			inAdj.get(dst).add(new Tuple2 <>(src, edge.getDstNodePort().intValue()));
			outAdj.get(src).add(dst);
			inDegree[dst]++;
		}

		for (int i = 0; i < v; ++i) {
			inAdj.get(i).sort(Comparator.comparing(o -> o.second));
		}

		PriorityQueue <Integer> queue = new PriorityQueue <>();
		int cnt = 0;

		for (int i = 0; i < v; ++i) {
			if (inDegree[i] == 0) {
				queue.add(i);
			}
		}

		while (!queue.isEmpty()) {
			int node = queue.remove();
			order[cnt++] = node;
			for (Integer t : outAdj.get(node)) {
				inDegree[t]--;

				if (inDegree[t] == 0) {
					queue.add(t);
				}
			}
		}

		if (cnt != v) {
			throw new RuntimeException("ERROR!");
		}

		Object[] objects = new Object[v];

		boolean hasBatchSink = false;
		boolean hasStreamOp = false;
		for (int i = 0; i < v; ++i) {
			int nodeId = order[i];
			Node node = nodes.get(nodeId);
			Map <String, String> paramMap = new HashMap <>();
			for (NodeParam nodeParam : nodeParamMap.get(node.getId())) {
				paramMap.put(nodeParam.getKey(), nodeParam.getValue());
			}
			paramMap.put(HasMLEnvironmentId.ML_ENVIRONMENT_ID.getName(), String.valueOf(mlEnvId));
			Params params = Params.fromJson(JsonConverter.toJson(paramMap));
			List <AlgoOperator <?>> ops = inAdj.get(nodeId).stream()
				.map(d -> (AlgoOperator <?>) (objects[d.first]))
				.collect(Collectors.toList());
			AlgoOperator <?> op = OpUtils.makeOp(node.getClassName(), params, ops);
			objects[nodeId] = op;

			hasBatchSink = hasBatchSink || op instanceof BaseSinkBatchOp;
			hasStreamOp = hasStreamOp || op instanceof StreamOperator;
		}

		try {
			if (hasStreamOp) {
				/*
				  When subclasses of ModelMapStreamOp exist in the plan, batch part of the plan may have already been
				  executed once due to {@link MemoryDataBridgeGenerator#generate}.
				  However, some BatchOperators may not be executed because of their orders in the node list.
				  For normal BatchOperators, it is OK to ignore them because they have no side effects.
				  But for those sinks, we have to ensure them to be executed.
				 */
				if (hasBatchSink) {
					try {
						mlEnv.getExecutionEnvironment().execute();
					} catch (RuntimeException e) {
						if (!e.getMessage().startsWith(
							"No new data sinks have been defined since the last execution.")) {
							throw e;
						}
					}
				}
				mlEnv.getStreamExecutionEnvironment().execute();
			} else {
				mlEnv.getExecutionEnvironment().execute();
			}
		} finally {
			MLEnvironmentFactory.remove(mlEnvId);
		}
	}

	@Override
	public void run(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams, Map <String, String> config)
		throws Exception {
		runImpl(nodes, edges, nodeParams);
	}

	public static class Tuple2<T1, T2> {
		public T1 first;
		public T2 second;

		public Tuple2(T1 first, T2 second) {
			this.first = first;
			this.second = second;
		}
	}
}
