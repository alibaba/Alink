package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class Node2VecWalkBatchOpTest extends AlinkTestBase {

	@Test
	public void testNode2Vec1() {
		GraphData g = GraphData.getGraph1();
		Node2VecWalkBatchOp node2VecWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setIsToUndigraph(false)
			.setSourceCol("start")
			.setTargetCol("dest");
		List <Row> walks = node2VecWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(8, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testNode2Vec2() {
		GraphData g = GraphData.getGraph2();
		Node2VecWalkBatchOp node2VecWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setIsToUndigraph(false)
			.setSourceCol("start")
			.setTargetCol("dest");
		List <Row> walks = node2VecWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(10, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testAliasWeightedNode2Vec1() {
		GraphData g = GraphData.getGraph1();
		Node2VecWalkBatchOp node2VecWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setWeightCol("weight")
			.setSamplingMethod("ALIAS");
		List <Row> walks = node2VecWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(8, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testAliasWeightedNode2Vec2() {
		GraphData g = GraphData.getGraph2();
		Node2VecWalkBatchOp node2VecWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setWeightCol("weight")
			.setSamplingMethod("ALIAS");
		List <Row> walks = node2VecWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(10, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testPartialSumWeightedNode2Vec1() {
		GraphData g = GraphData.getGraph1();
		Node2VecWalkBatchOp node2VecWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setWeightCol("weight")
			.setSamplingMethod("PARTIAL_SUM");
		List <Row> walks = node2VecWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(8, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testPartialSumWeightedNode2Vec2() {
		GraphData g = GraphData.getGraph2();
		Node2VecWalkBatchOp node2VecWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setWeightCol("weight")
			.setSamplingMethod("PARTIAL_SUM");
		List <Row> walks = node2VecWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(10, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}
}