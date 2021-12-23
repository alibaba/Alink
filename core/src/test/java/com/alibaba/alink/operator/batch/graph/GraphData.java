package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphData {

	/**
	 * All node have outgoing neighbors
	 *
	 * @return
	 */
	public static GraphData getGraph1() {
		List <GraphEdge> edgeList = new ArrayList <>();
		edgeList.add(new GraphEdge(1, 2, 1.0, 'a', 'b'));
		edgeList.add(new GraphEdge(2, 3, 1.0, 'b', 'c'));
		edgeList.add(new GraphEdge(3, 4, 1.0, 'c', 'a'));
		edgeList.add(new GraphEdge(4, 1, 3.0, 'a', 'a'));
		edgeList.add(new GraphEdge(1, 3, 1.0, 'a', 'c'));
		edgeList.add(new GraphEdge(2, 4, 1.0, 'b', 'a'));

		Map <Long, Character> nodeAndTypes = new HashMap <>();
		nodeAndTypes.put(1L, 'a');
		nodeAndTypes.put(2L, 'b');
		nodeAndTypes.put(3L, 'c');
		nodeAndTypes.put(4L, 'a');

		return new GraphData(edgeList, nodeAndTypes);
	}

	/**
	 * Some node has no outgoing neighbors
	 *
	 * @return
	 */
	public static GraphData getGraph2() {
		List <GraphEdge> edgeList = new ArrayList <>();
		edgeList.add(new GraphEdge(1, 2, 1.0, 'a', 'b'));
		edgeList.add(new GraphEdge(2, 3, 1.0, 'b', 'c'));
		edgeList.add(new GraphEdge(3, 4, 1.0, 'c', 'a'));
		edgeList.add(new GraphEdge(4, 5, 3.0, 'a', 'b'));
		edgeList.add(new GraphEdge(5, 6, 1.0, 'b', 'c'));

		Map <Long, Character> nodeAndTypes = new HashMap <>();
		nodeAndTypes.put(1L, 'a');
		nodeAndTypes.put(2L, 'b');
		nodeAndTypes.put(3L, 'c');
		nodeAndTypes.put(4L, 'a');
		nodeAndTypes.put(5L, 'b');
		nodeAndTypes.put(6L, 'c');

		return new GraphData(edgeList, nodeAndTypes);
	}

	final List <GraphEdge> edgeList;
	final Map <Long, Character> nodeAndTypes;

	public GraphData(List <GraphEdge> edgeList, Map <Long, Character> nodeAndTypes) {
		this.edgeList = edgeList;
		this.nodeAndTypes = nodeAndTypes;
	}

	public List <GraphEdge> getEdgeList() {
		return edgeList;
	}

	public Map <Long, Character> getNodeAndTypes() {
		return nodeAndTypes;
	}

	public List <GraphEdge> getHomoSortedEdgeList() {
		List <GraphEdge> edges = getEdgeList();
		Collections.sort(edges, new Comparator <GraphEdge>() {
			@Override
			public int compare(GraphEdge o1, GraphEdge o2) {
				return (int) (o1.getSource() - o2.getSource() == 0 ? o1.getTarget() - o2.getTarget()
					: o1.getSource() - o2.getSource());
			}
		});
		return edges;
	}

	public List <GraphEdge> getHeteSortedEdgeList() {
		List <GraphEdge> edges = getEdgeList();
		Collections.sort(edges);
		return edges;
	}

	public List <Row> getEdgeRowList() {
		List <Row> res = new ArrayList <>();
		for (GraphEdge edge : getEdgeList()) {
			res.add(Row.of(edge.getSource(), edge.getTarget(), edge.getValue()));
		}
		return res;
	}

	public int getEdgeNum() {
		return edgeList.size();
	}

	public int getSrcVertexNum() {
		Set <Long> srcVertices = new HashSet <>();
		for (GraphEdge edge : getEdgeList()) {
			srcVertices.add(edge.getSource());
		}

		return srcVertices.size();
	}

	public MemSourceBatchOp getGraphOp() {
		return new MemSourceBatchOp(getEdgeRowList(), "start long, dest long, weight double");
	}

	public MemSourceBatchOp getNodeAndTypesOp() {
		Map <Long, Character> map = getNodeAndTypes();
		List <Row> nodeTypes = new ArrayList <>();
		for (Map.Entry <Long, Character> e : map.entrySet()) {
			nodeTypes.add(Row.of(e.getKey(), e.getValue().toString()));
		}
		return new MemSourceBatchOp(nodeTypes, "node long, type string");
	}

	public List <Character> getNodeTypeList() {
		Set <Character> nodeTypes = new HashSet <>();
		for (Character c : getNodeAndTypes().values()) {
			nodeTypes.add(c);
		}
		return new ArrayList <>(nodeTypes);
	}
}
