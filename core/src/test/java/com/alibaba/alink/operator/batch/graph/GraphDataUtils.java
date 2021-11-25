package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.graph.storage.HomoGraphEngine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

public class GraphDataUtils {

	public static boolean checkWalksValid(List <Row> walks, int walkLen, GraphData g) {
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(), g.getSrcVertexNum(),
			g.getEdgeNum(),
			true);
		for (Row walk : walks) {
			String[] nodes = ((String) walk.getField(0)).split(" ");
			long[] longNodes = Arrays.stream(nodes).mapToLong(new ToLongFunction <String>() {
				@Override
				public long applyAsLong(String value) {
					return Long.parseLong(value);
				}
			}).toArray();

			for (int i = 1; i < longNodes.length; i++) {
				if (!homoGraphEngine.containsEdge(longNodes[i - 1], longNodes[i])) {
					return false;
				}
			}
		}
		return true;
	}

	public static boolean checkHeteWalksValid(List <Row> walks, String[] metaPath, int walkLen, GraphData g) {
		if (!checkWalksValid(walks, walkLen, g)) {
			return false;
		}
		for (Row walk : walks) {
			String[] nodes = ((String) walk.getField(0)).split(" ");
			long[] longNodes = Arrays.stream(nodes).mapToLong(new ToLongFunction <String>() {
				@Override
				public long applyAsLong(String value) {
					return Long.parseLong(value);
				}
			}).toArray();

			boolean findMetaPath = false;
			for (int m = 0; m < metaPath.length; m++) {
				if (checkMetaPath(metaPath[m], longNodes, g.getNodeAndTypes())) {
					findMetaPath = true;
					break;
				}
			}
			if (!findMetaPath) {
				return false;
			}
		}
		return true;
	}

	private static boolean checkMetaPath(String metaPath, long[] walk, Map <Long, Character> nodeAndTypes) {
		for (int i = 0; i < walk.length; i++) {
			int nodeTypeId = i % (metaPath.length() - 1);
			nodeTypeId = nodeTypeId == -1 ? 0 : nodeTypeId;
			if (metaPath.charAt(nodeTypeId) != nodeAndTypes.get(walk[i])) {
				return false;
			}
		}
		return true;
	}

}
