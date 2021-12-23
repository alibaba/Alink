package com.alibaba.alink.operator.batch.graph.storage;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.graph.GraphData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class HeteGraphEngineTest extends AlinkTestBase {

	@Test
	public void testUniformSample() {
		GraphData g = GraphData.getGraph1();
		HeteGraphEngine heteGraphEngine = new HeteGraphEngine(g.getHeteSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), g.getNodeTypeList(), false, false);
		assertEquals(-1, heteGraphEngine.sampleOneNeighbor(1, 'a'));
		assertEquals(2, heteGraphEngine.sampleOneNeighbor(1, 'b'));
		assertEquals(3, heteGraphEngine.sampleOneNeighbor(1, 'c'));
		assertEquals(3, heteGraphEngine.sampleOneNeighbor(2, 'c'));
		assertEquals(4, heteGraphEngine.sampleOneNeighbor(2, 'a'));
		assertEquals(4, heteGraphEngine.sampleOneNeighbor(3, 'a'));
		assertEquals(1, heteGraphEngine.sampleOneNeighbor(4, 'a'));

	}

	@Test
	public void testWeightedPartialSumSample() {
		GraphData g = GraphData.getGraph1();
		HeteGraphEngine heteGraphEngine = new HeteGraphEngine(g.getHeteSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), g.getNodeTypeList(), true, false);
		assertEquals(-1, heteGraphEngine.sampleOneNeighbor(1, 'a'));
		assertEquals(2, heteGraphEngine.sampleOneNeighbor(1, 'b'));
		assertEquals(3, heteGraphEngine.sampleOneNeighbor(1, 'c'));
		assertEquals(3, heteGraphEngine.sampleOneNeighbor(2, 'c'));
		assertEquals(4, heteGraphEngine.sampleOneNeighbor(2, 'a'));
		assertEquals(4, heteGraphEngine.sampleOneNeighbor(3, 'a'));
		assertEquals(1, heteGraphEngine.sampleOneNeighbor(4, 'a'));
	}

	@Test
	public void testWeightedAliasSample() {
		GraphData g = GraphData.getGraph1();
		HeteGraphEngine heteGraphEngine = new HeteGraphEngine(g.getHeteSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), g.getNodeTypeList(), true, false);
		assertEquals(-1, heteGraphEngine.sampleOneNeighbor(1, 'a'));
		assertEquals(2, heteGraphEngine.sampleOneNeighbor(1, 'b'));
		assertEquals(3, heteGraphEngine.sampleOneNeighbor(1, 'c'));
		assertEquals(3, heteGraphEngine.sampleOneNeighbor(2, 'c'));
		assertEquals(4, heteGraphEngine.sampleOneNeighbor(2, 'a'));
		assertEquals(4, heteGraphEngine.sampleOneNeighbor(3, 'a'));
		assertEquals(1, heteGraphEngine.sampleOneNeighbor(4, 'a'));
	}

	@Test
	public void testGetAllSrcVerticesWithTypes() {
		GraphData g = GraphData.getGraph1();
		HeteGraphEngine heteGraphEngine = new HeteGraphEngine(g.getHeteSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), g.getNodeTypeList(), false, false);
		Iterator <Tuple2 <Long, Character>> allSrcVerticesWithTypes = heteGraphEngine.getAllSrcVerticesWithTypes();
		int cnt = 0;
		while (allSrcVerticesWithTypes.hasNext()) {
			Tuple2 <Long, Character> vertexWithType = allSrcVerticesWithTypes.next();
			cnt++;
		}
		assertEquals(4, cnt);
	}

}