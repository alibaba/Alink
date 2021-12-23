package com.alibaba.alink.operator.batch.graph.storage;

import com.alibaba.alink.operator.batch.graph.GraphData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class HomoGraphEngineTest extends AlinkTestBase {

	@Test
	public void testWeightedLoad() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), true);
		assertArrayEquals(homoGraphEngine.srcEnds, new int[] {2, 4, 5, 6});
		assertArrayEquals(homoGraphEngine.dst, new long[] {2, 3, 3, 4, 4, 1});
		// partial sums
		assertArrayEquals(homoGraphEngine.weights, new double[] {0.5, 1, 0.5, 1, 1, 1}, 1e-9);
	}

	@Test
	public void testUniformLoad() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), false);
		assertArrayEquals(homoGraphEngine.srcEnds, new int[] {2, 4, 5, 6});
		assertArrayEquals(homoGraphEngine.dst, new long[] {2, 3, 3, 4, 4, 1});
		assert null == homoGraphEngine.weights;
	}

	@Test
	public void testContainsKey() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), false);
		assertTrue(homoGraphEngine.containsVertex(1));
		assertFalse(homoGraphEngine.containsVertex(10));
	}

	@Test
	public void testUniformSample() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), false);

		long n1 = homoGraphEngine.sampleOneNeighbor(1);
		assertTrue(n1 == 2 || n1 == 3);
		long n2 = homoGraphEngine.sampleOneNeighbor(2);
		assertTrue(n2 == 3 || n2 == 4);
		long n3 = homoGraphEngine.sampleOneNeighbor(3);
		assertEquals(4, n3);
		long n4 = homoGraphEngine.sampleOneNeighbor(4);
		assertEquals(1, n4);

	}

	@Test
	public void testWeightedPartialSumSample() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), true, false);
		long n1 = homoGraphEngine.sampleOneNeighbor(1);
		assertTrue(n1 == 2 || n1 == 3);
		long n2 = homoGraphEngine.sampleOneNeighbor(2);
		assertTrue(n2 == 3 || n2 == 4);
		long n3 = homoGraphEngine.sampleOneNeighbor(3);
		assertEquals(4, n3);
		long n4 = homoGraphEngine.sampleOneNeighbor(4);
		assertEquals(1, n4);
	}

	@Test
	public void testWeightedAliasSample() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), true, true);
		long n1 = homoGraphEngine.sampleOneNeighbor(1);
		assertTrue(n1 == 2 || n1 == 3);
		long n2 = homoGraphEngine.sampleOneNeighbor(2);
		assertTrue(n2 == 3 || n2 == 4);
		long n3 = homoGraphEngine.sampleOneNeighbor(3);
		assertEquals(4, n3);
		long n4 = homoGraphEngine.sampleOneNeighbor(4);
		assertEquals(1, n4);
	}

	@Test
	public void testContainsEdge() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), true, false);
		for (GraphEdge e : g.getEdgeList()) {
			long src = e.getSource();
			long dst = e.getTarget();
			assertTrue(homoGraphEngine.containsEdge(src, dst));
		}

		assertFalse(homoGraphEngine.containsEdge(1, 1));
	}

	@Test
	public void testGetNumNeighbors() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), true, false);
		assertEquals(2, homoGraphEngine.getNumNeighbors(1));
		assertEquals(2, homoGraphEngine.getNumNeighbors(2));
		assertEquals(1, homoGraphEngine.getNumNeighbors(3));
		assertEquals(1, homoGraphEngine.getNumNeighbors(4));
		assertEquals(0, homoGraphEngine.getNumNeighbors(5));
	}

	@Test
	public void testGetPhysicalWorkerIdByLogicalWorkerId() {
		GraphData g = GraphData.getGraph1();
		HomoGraphEngine homoGraphEngine = new HomoGraphEngine(g.getHomoSortedEdgeList(),
			g.getSrcVertexNum(), g.getEdgeNum(), true, false);
		Map <Integer, Integer> map = new HashMap <>();
		map.put(1, 1);
		homoGraphEngine.setLogicalWorkerIdToPhysicalWorkerId(map);
		assertTrue(homoGraphEngine.getPhysicalWorkerIdByLogicalWorkerId(1) == 1);
		assertTrue(homoGraphEngine.getPhysicalWorkerIdByLogicalWorkerId(5) == 0);
	}
}