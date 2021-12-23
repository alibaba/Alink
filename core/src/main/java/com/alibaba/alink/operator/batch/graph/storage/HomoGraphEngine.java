package com.alibaba.alink.operator.batch.graph.storage;

import java.util.Arrays;

/**
 * Stores weighted homogeneous graph in CSR format. Supports sampling a neighbor of a given vertex.
 */
public class HomoGraphEngine extends BaseCSRGraph {

	public HomoGraphEngine(Iterable <GraphEdge> sortedEdges, int srcVertexNum, int edgeNum,
						   boolean isWeighted) {
		this(sortedEdges, srcVertexNum, edgeNum, isWeighted, false);
	}

	/**
	 * Requirement: the input edges are sorted by srcId, then dstId.
	 *
	 * @param sortedEdges   the sorted input edges
	 * @param srcVertexNum  number of vertices
	 * @param edgeNum       number of edges
	 * @param isWeighted    whether it is a weighted graph or not
	 * @param useAliasTable whether we use alias table
	 */
	public HomoGraphEngine(Iterable <GraphEdge> sortedEdges, int srcVertexNum, int edgeNum,
						   boolean isWeighted, boolean useAliasTable) {
		super(srcVertexNum, edgeNum, isWeighted, useAliasTable);
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
		int curIdxInSrcEnds = 0;
		for (GraphEdge e : sortedEdges) {
			long srcId = e.getSource();
			long dstId = e.getTarget();
			double weight = e.getValue();
			if (!srcVertexId2LocalId.containsKey(srcId)) {
				srcVertexId2LocalId.put(srcId, srcVertexId2LocalId.size());
			}

			// update prevVertexId & srcEnds[curIdxInSrcEnds] if we finished reading one vertex's neighbor.
			int currentVertexId = srcVertexId2LocalId.get(srcId);
			if (currentVertexId != prevVertexId) {
				// special case for the first vertex.
				if (prevVertexId != -1) {
					srcEnds[curIdxInSrcEnds] = curIdxInDst;
					curIdxInSrcEnds++;
				}
				prevVertexId = currentVertexId;
			}
			// add the edge
			dst[curIdxInDst] = dstId;
			if (isWeighted) {
				weights[curIdxInDst] = weight;
			}
			curIdxInDst++;
		}
		// update last vertex
		if (srcEnds.length > 0) {
			srcEnds[curIdxInSrcEnds] = curIdxInDst;
		}
	}

	/**
	 * sample one neighbor of the given vertexId.
	 *
	 * @param vertexId
	 * @return
	 */
	public long sampleOneNeighbor(long vertexId) {
		int localId = srcVertexId2LocalId.get(vertexId);
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
			resLocalId = random.nextDouble() < prob[start + k] ? start + k : alias[start + k];
		} else {
			// partialSum sampling
			double next = random.nextDouble();
			resLocalId = Arrays.binarySearch(partialSum, start, end, next);
			resLocalId = Math.abs(resLocalId) - 1;
		}
		return dst[resLocalId];
	}

	public int getNumNeighbors(long vertexId) {
		if (srcVertexId2LocalId.containsKey(vertexId)) {
			int localId = srcVertexId2LocalId.get(vertexId);
			int start = localId == 0 ? 0 : srcEnds[localId - 1];
			int end = srcEnds[localId];
			return end - start;
		} else {
			return 0;
		}
	}

	/**
	 * whether there is an edge from src to dst
	 *
	 * @param srcVertexId
	 * @param dstVertexId
	 * @return
	 */
	public boolean containsEdge(long srcVertexId, long dstVertexId) {
		int localId = srcVertexId2LocalId.get(srcVertexId);
		int start = localId == 0 ? 0 : srcEnds[localId - 1];
		int end = srcEnds[localId];
		int index = Arrays.binarySearch(dst, start, end, dstVertexId);
		if (index >= 0 && dst[index] == dstVertexId) {
			return true;
		}
		return false;
	}
}