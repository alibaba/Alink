package com.alibaba.alink.operator.batch.graph.storage;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class GraphEdgeTest extends AlinkTestBase {

	@Test
	public void test() {

		List <GraphEdge> edges = new ArrayList <>();
		edges.add(new GraphEdge(1, 2, 1.0, null, 'b'));
		edges.add(new GraphEdge(2, 3, 1.0, null, 'c'));
		edges.add(new GraphEdge(3, 4, 1.0, null, 'a'));
		edges.add(new GraphEdge(4, 1, 3.0, null, 'a'));
		edges.add(new GraphEdge(1, 3, 1.0, null, 'c'));
		edges.add(new GraphEdge(2, 4, 1.0, null, 'a'));
		Collections.sort(edges);

		assertEquals(edges.get(0).getSource(), 1);
		assertEquals(edges.get(0).getTarget(), 2);
		assertEquals(edges.get(0).getValue(), 1, 1e-9);
		assertEquals(edges.get(5).getSource(), 4);
		assertEquals(edges.get(5).getTarget(), 1);
		assertEquals(edges.get(5).getValue(), 3, 1e-9);
	}
}
