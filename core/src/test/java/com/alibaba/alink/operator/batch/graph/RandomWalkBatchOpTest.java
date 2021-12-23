package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class RandomWalkBatchOpTest extends AlinkTestBase {

	@Test
	public void testRandomWalk1() {
		GraphData g = GraphData.getGraph1();
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setIsToUndigraph(false)
			.setSourceCol("start")
			.setTargetCol("dest");
		List <Row> walks = randomWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(8, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testRandomWalk2() {
		GraphData g = GraphData.getGraph2();
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setIsToUndigraph(false)
			.setSourceCol("start")
			.setTargetCol("dest");
		List <Row> walks = randomWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(10, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testAliasWeightedRandomWalk1() {
		GraphData g = GraphData.getGraph1();
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setIsWeightedSampling(true)
			.setWeightCol("weight")
			.setSamplingMethod("ALIAS");
		List <Row> walks = randomWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(8, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testAliasWeightedRandomWalk2() {
		GraphData g = GraphData.getGraph2();
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setIsWeightedSampling(true)
			.setWeightCol("weight")
			.setSamplingMethod("ALIAS");
		List <Row> walks = randomWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(10, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testPartialSumWeightedRandomWalk1() {
		GraphData g = GraphData.getGraph1();
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setIsWeightedSampling(true)
			.setWeightCol("weight")
			.setSamplingMethod("PARTIAL_SUM");
		List <Row> walks = randomWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(8, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}

	@Test
	public void testPartialSumWeightedRandomWalk2() {
		GraphData g = GraphData.getGraph2();
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setIsWeightedSampling(true)
			.setWeightCol("weight")
			.setSamplingMethod("PARTIAL_SUM");
		List <Row> walks = randomWalkBatchOp.linkFrom(g.getGraphOp()).collect();
		assertEquals(10, walks.size());
		assertTrue(GraphDataUtils.checkWalksValid(walks, 5, g));
	}
}
