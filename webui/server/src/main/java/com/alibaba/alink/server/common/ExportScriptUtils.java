package com.alibaba.alink.server.common;

import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class ExportScriptUtils {

	private static final int INDENT_SIZE = 4;
	private static final String INDENT_STR = StringUtils.repeat(" ", INDENT_SIZE);

	private static final Map <String, String> PARAM_VALUE_MAP = new HashMap <>();

	static {
		PARAM_VALUE_MAP.put("true", "True");
		PARAM_VALUE_MAP.put("false", "False");
	}

	private static String convertParamValue(String value) {
		for (Entry <String, String> entry : PARAM_VALUE_MAP.entrySet()) {
			if (entry.getKey().equals(value)) {
				return entry.getValue();
			}
		}
		return value;
	}

	/**
	 * Generate a name for operator to avoid conflicts.
	 *
	 * @param originalName
	 * @param names
	 * @return
	 */
	static String generateOpName(String originalName, Collection <String> names) {
		if (originalName.contains(" ")) {
			originalName = originalName.replaceAll(" ", "");
		}
		if (!names.contains(originalName)) {
			return originalName;
		}
		int INDEX_LIMIT = 100;
		for (int i = 1; i < INDEX_LIMIT; i += 1) {
			String name = originalName + i;
			if (!names.contains(name)) {
				return name;
			}
		}
		throw new RuntimeException(String.format("Cannot get a valid name for %s", originalName));
	}

	private static List <Long> topologicalSort(List <Edge> edges) {
		Set <Long> nodeIdSet = new HashSet <>();
		Map <Long, List <Edge>> outEdges = new HashMap <>();
		Map <Long, Long> inDegrees = new HashMap <>();

		for (Edge edge : edges) {
			Long srcNodeId = edge.getSrcNodeId();
			Long dstNodeId = edge.getDstNodeId();
			nodeIdSet.add(srcNodeId);
			nodeIdSet.add(dstNodeId);
			outEdges.putIfAbsent(srcNodeId, new ArrayList <>());
			outEdges.get(srcNodeId).add(edge);
			inDegrees.merge(dstNodeId, 1L, (oldV, v) -> oldV + 1);
		}

		Queue <Long> q = new ArrayDeque <>();
		for (Long nodeId : nodeIdSet) {
			if (inDegrees.getOrDefault(nodeId, 0L).equals(0L)) {
				q.add(nodeId);
			}
		}

		List <Long> sorted = new ArrayList <>();
		while (!q.isEmpty()) {
			Long srcNodeId = q.poll();
			sorted.add(srcNodeId);
			if (!outEdges.containsKey(srcNodeId)) {
				continue;
			}
			for (Edge edge : outEdges.get(srcNodeId)) {
				Long dstNodeId = edge.getDstNodeId();
				Long inDegree = inDegrees.get(dstNodeId);
				inDegree -= 1;
				if (inDegree.equals(0L)) {
					inDegrees.remove(dstNodeId);
					q.add(dstNodeId);
				} else {
					inDegrees.put(dstNodeId, inDegree);
				}
			}
		}
		return sorted;
	}

	private static List <String> generateOpScript(Class <?> clz, Long nodeId, Node node,
												  Map <Long, String> nodeId2Name, List <NodeParam> nodeParams,
												  List <Edge> inEdges) {
		String name = generateOpName(node.getName(), nodeId2Name.values());
		nodeId2Name.put(nodeId, name);

		@SuppressWarnings("unchecked")
		boolean hasModelOpConstructor = StreamOperator.class.isAssignableFrom(clz) &&
			OpUtils.hasModelOpConstructor((Class <? extends StreamOperator <?>>) clz);

		List <String> lines = new ArrayList <>();
		if (hasModelOpConstructor) {
			lines.add(String.format("%s = %s(%s)", name, clz.getSimpleName(),
				nodeId2Name.get(inEdges.get(0).getSrcNodeId())));
			inEdges = inEdges.subList(1, inEdges.size());
		} else {
			lines.add(String.format("%s = %s()", name, clz.getSimpleName()));
		}
		for (NodeParam nodeParam : nodeParams) {
			String value = nodeParam.getValue();
			value = convertParamValue(value);
			lines.add(INDENT_STR + String.format(".set%s(%s)",
				StringUtils.capitalize(nodeParam.getKey()),
				value));
		}

		if (inEdges.size() > 0) {
			List <String> inNodeNames = inEdges.stream()
				.map(d -> nodeId2Name.get(d.getSrcNodeId()))
				.collect(Collectors.toList());
			lines.add(INDENT_STR + String.format(".linkFrom(%s)",
				String.join(", ", inNodeNames)));
		}

		for (int i = 0; i < lines.size() - 1; i += 1) {
			lines.set(i, lines.get(i) + " \\");
		}
		return lines;
	}

	/**
	 * Generate PyAlink script for the DAG part, not including header part.
	 *
	 * @param nodes
	 * @param edges
	 * @param nodeParams
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static List <String> generateDAGScript(List <Node> nodes, List <Edge> edges, List <NodeParam> nodeParams)
		throws ClassNotFoundException {
		Map <Long, Node> id2NodeMap = new HashMap <>();
		for (Node node : nodes) {
			id2NodeMap.put(node.getId(), node);
		}

		Map <Long, List <Edge>> outEdges = new HashMap <>();
		Map <Long, List <Edge>> inEdges = new HashMap <>();
		for (Edge edge : edges) {
			Long srcNodeId = edge.getSrcNodeId();
			Long dstNodeId = edge.getDstNodeId();
			outEdges.putIfAbsent(srcNodeId, new ArrayList <>());
			inEdges.putIfAbsent(dstNodeId, new ArrayList <>());
			outEdges.get(srcNodeId).add(edge);
			inEdges.get(dstNodeId).add(edge);
		}

		for (List <Edge> edgeList : inEdges.values()) {
			edgeList.sort(Comparator.comparing(Edge::getDstNodePort));
		}

		Map <Long, List <NodeParam>> id2NodeParams = new HashMap <>();
		for (NodeParam nodeParam : nodeParams) {
			Long nodeId = nodeParam.getNodeId();
			id2NodeParams.putIfAbsent(nodeId, new ArrayList <>());
			id2NodeParams.get(nodeId).add(nodeParam);
		}

		List <Long> sortedNodeIds = topologicalSort(edges);
		Map <Long, String> nodeId2Name = new HashMap <>();

		List <String> lines = new ArrayList <>();
		boolean hasStreamOp = false;
		boolean hasBatchSink = false;
		for (Long nodeId : sortedNodeIds) {
			Node node = id2NodeMap.get(nodeId);
			Class <?> clz = Class.forName(node.getClassName());
			List <String> opLines = generateOpScript(
				clz,
				nodeId, node, nodeId2Name,
				id2NodeParams.getOrDefault(nodeId, Collections.emptyList()),
				inEdges.getOrDefault(nodeId, Collections.emptyList()));
			lines.addAll(opLines);
			hasBatchSink = hasBatchSink || BaseSinkBatchOp.class.isAssignableFrom(clz);
			hasStreamOp = hasStreamOp || StreamOperator.class.isAssignableFrom(clz);
		}
		lines.addAll(generateExecuteScript(hasStreamOp, hasBatchSink));
		return lines;
	}

	static List <String> generateExecuteScript(boolean hasStreamOp, boolean hasBatchSink) {
		List <String> lines = new ArrayList <>();
		if (hasStreamOp) {
			if (hasBatchSink) {
				lines.add("try:");
				lines.add(INDENT_STR + "BatchOperator.execute()");
				lines.add("except Exception as ex:");
				lines.add(INDENT_STR
					+ "if 'No new data sinks have been defined since the last execution.' not in ex.java_exception.getMessage():");
				lines.add(INDENT_STR + INDENT_STR + "raise ex");
			}
			lines.add("StreamOperator.execute()");
		} else {
			lines.add("BatchOperator.execute()");
		}
		return lines;
	}
}
