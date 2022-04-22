package com.alibaba.alink.operator.batch.graph.storage;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * stores weighted hete graph in CSR format. Supports sampling a node's neighbor with specific nodeType.
 */
public class HeteGraphEngine extends BaseCSRGraph {

	HashMap <Character, Integer> nodeType2TypeId;
	List <Character> nodeTypes;
	/**
	 * local Id of source vertex --> nodeType
	 */
	Character[] srcNodeTypes;

	public HeteGraphEngine(Iterable <GraphEdge> sortedEdges, int srcVertexNum, int edgeNum,
						   List <Character> nodeTypes, boolean isWeighted) {
		this(sortedEdges, srcVertexNum, edgeNum, nodeTypes, isWeighted, false);
	}

	/**
	 * Requirement: the input edges are sorted by srcId, then type, then dstId.
	 *
	 * @param sortedEdges  the sorted input edges
	 * @param srcVertexNum number of vertices
	 * @param edgeNum      number of edges
	 * @param nodeTypes    all possible node types
	 * @param isWeighted   whether it is a weighted graph or not
	 */
	public HeteGraphEngine(
		Iterable <GraphEdge> sortedEdges, int srcVertexNum, int edgeNum,
		List <Character> nodeTypes, boolean isWeighted, boolean useAliasTable) {
		super(srcVertexNum, edgeNum, isWeighted, useAliasTable, nodeTypes.size());
		this.srcNodeTypes = new Character[srcVertexNum];
		this.nodeTypes = nodeTypes;
		// sort the node types in ascending order.
		Collections.sort(nodeTypes);
		int numNodeTypes = nodeTypes.size();
		nodeType2TypeId = new HashMap <>(numNodeTypes * 3 / 2);
		for (int i = 0; i < numNodeTypes; i++) {
			nodeType2TypeId.put(nodeTypes.get(i), i);
		}
		readSortedEdges(sortedEdges);
		if (isWeighted) {
			for (int i = 0; i < srcEnds.length; i++) {
				buildOneVertexPartialSumOrAliasTable(i);
			}
		}
	}

	/**
	 * build the graph in CSR format
	 *
	 * @param sortedEdges
	 */
	private void readSortedEdges(Iterable <GraphEdge> sortedEdges) {
		// read in data
		int prevVertexId = -1;
		int curIdxInDst = 0;
		int curIdxInSrc = 0;
		int curNodeTypeId = 0;
		Character prevNodeType = null;

		for (GraphEdge graphEdge : sortedEdges) {
			long srcId = graphEdge.getSource();
			long dstId = graphEdge.getTarget();
			double weight = graphEdge.getValue();
			Character srcType = graphEdge.getSrcType();
			Character dstType = graphEdge.getDstType();
			if (!srcVertexId2LocalId.containsKey(srcId)) {
				int newLocalId = srcVertexId2LocalId.size();
				srcNodeTypes[newLocalId] = srcType;
				srcVertexId2LocalId.put(srcId, srcVertexId2LocalId.size());
			}

			// update prevVertexId & src[curIdxInSrc] if we finshed reading one vertex's neighbor.
			int currentVertexId = srcVertexId2LocalId.get(srcId);
			if (currentVertexId != prevVertexId) {
				// special case for the first vertex.
				// if we are dealing with the first edge in first vertex, we do nothing here.
				// otherwise we update the srcEnds.
				if (prevVertexId != -1) {
					// dealing with the empty nodeTypes of the previous vertex.
					while (curNodeTypeId != nodeTypes.size()) {
						srcEnds[curIdxInSrc] = curIdxInDst;
						curIdxInSrc++;
						curNodeTypeId++;
					}
				}
				// it is the first vertex.
				prevVertexId = currentVertexId;
				prevNodeType = nodeTypes.get(0);
			}

			curNodeTypeId = curNodeTypeId % nodeTypes.size();
			// if node type has changed, update srcEnds.
			while (!dstType.equals(prevNodeType)) {
				curNodeTypeId++;
				prevNodeType = nodeTypes.get(curNodeTypeId);
				srcEnds[curIdxInSrc] = curIdxInDst;
				curIdxInSrc++;
			}

			curNodeTypeId = curNodeTypeId % nodeTypes.size();
			// add the edge
			dst[curIdxInDst] = dstId;
			if (isWeighted) {
				weights[curIdxInDst] = (Double) weight;
			}
			curIdxInDst++;
		}
		// update last vertex
		while (curIdxInSrc < srcEnds.length) {
			srcEnds[curIdxInSrc++] = curIdxInDst;
		}
	}

	/**
	 * get node type of ${vertexId}. Note that vertexId must be in srcVertexId2LocalId.
	 *
	 * @param vertexId
	 * @return
	 */
	public Character getNodeType(long vertexId) {
		int localId = srcVertexId2LocalId.get(vertexId);
		return srcNodeTypes[localId];
	}

	/**
	 * get all source vertices and their corresponding node types
	 *
	 * @return
	 */
	public Iterator <Tuple2 <Long, Character>> getAllSrcVerticesWithTypes() {
		Iterator <Long> srcVertices = srcVertexId2LocalId.keySet().iterator();
		return new Iterator <Tuple2 <Long, Character>>() {
			@Override
			public boolean hasNext() {
				return srcVertices.hasNext();
			}

			@Override
			public Tuple2 <Long, Character> next() {
				long nextVertexId = srcVertices.next();
				char nextVertexType = getNodeType(nextVertexId);
				return Tuple2.of(nextVertexId, nextVertexType);
			}
		};
	}

	/**
	 * get one sample of ${vertexId} with type as ${dstType}
	 *
	 * @param vertexId
	 * @param dstType
	 * @return
	 */
	public long sampleOneNeighbor(long vertexId, char dstType) {
		if (!nodeType2TypeId.containsKey(dstType)) {
			return -1;
		}
		int localId = srcVertexId2LocalId.get(vertexId) * nodeTypes.size() + nodeType2TypeId.get(dstType);
		int start = localId == 0 ? 0 : srcEnds[localId - 1];
		int end = srcEnds[localId];
		if (start == end) {
			return -1;
		}

		int resLocalId = 0;

		if (!isWeighted) {
			// uniform sampling
			resLocalId = random.nextInt(end - start) + start;
		} else if (useAliasTable) {
			// alias sampling
			int k = random.nextInt(end - start);
			resLocalId = random.nextDouble() < prob[start + k] ? start + k : alias[start + k] + start;
		} else {
			// partialSum sampling
			double next = random.nextDouble();
			resLocalId = Arrays.binarySearch(partialSum, start, end, next);
			if (resLocalId < 0) {
				resLocalId = -resLocalId - 1;
			}
		}
		return dst[resLocalId];
	}
}
