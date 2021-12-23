package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.List;

public class MetaPathWalkBatchOpTest extends AlinkTestBase {

	@Test
	public void testMetaPathWalk1() {
		GraphData g = GraphData.getGraph1();
		List <Row> walks = new MetaPathWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setMetaPath("aba")
			.setVertexCol("node")
			.setTypeCol("type")
			.linkFrom(g.getGraphOp(), g.getNodeAndTypesOp()).collect();
		assertEquals(4, walks.size());
		assertTrue(GraphDataUtils.checkHeteWalksValid(walks, new String[]{"aba"}, 5, g));
	}

	@Test
	public void testMetaPathWalk2() {
		GraphData g = GraphData.getGraph2();
		List <Row> walks = new MetaPathWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setMetaPath("abca")
			.setVertexCol("node")
			.setTypeCol("type")
			.linkFrom(g.getGraphOp(), g.getNodeAndTypesOp()).collect();
		assertEquals(4, walks.size());
		assertTrue(GraphDataUtils.checkHeteWalksValid(walks, new String[]{"abca"}, 5, g));
	}

	@Test
	public void testMetaPathWalkAlias1() {
		GraphData g = GraphData.getGraph1();
		List <Row> walks = new MetaPathWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setMetaPath("aba")
			.setVertexCol("node")
			.setWeightCol("weight")
			.setSamplingMethod("ALIAS")
			.setTypeCol("type")
			.linkFrom(g.getGraphOp(), g.getNodeAndTypesOp()).collect();
		assertEquals(4, walks.size());
		assertTrue(GraphDataUtils.checkHeteWalksValid(walks, new String[]{"aba"}, 5, g));
	}

	@Test
	public void testMetaPathWalkAlias2() {
		GraphData g = GraphData.getGraph2();
		List <Row> walks = new MetaPathWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setMetaPath("abca")
			.setVertexCol("node")
			.setWeightCol("weight")
			.setSamplingMethod("ALIAS")
			.setTypeCol("type")
			.linkFrom(g.getGraphOp(), g.getNodeAndTypesOp()).collect();
		assertEquals(4, walks.size());
		assertTrue(GraphDataUtils.checkHeteWalksValid(walks, new String[]{"abca"}, 5, g));
	}

	@Test
	public void testMetaPathWalkPartialSum1() {
		GraphData g = GraphData.getGraph1();
		List <Row> walks = new MetaPathWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setMetaPath("aba")
			.setVertexCol("node")
			.setWeightCol("weight")
			.setSamplingMethod("PARTIALSUM")
			.setTypeCol("type")
			.linkFrom(g.getGraphOp(), g.getNodeAndTypesOp()).collect();
		assertEquals(4, walks.size());
		assertTrue(GraphDataUtils.checkHeteWalksValid(walks, new String[]{"aba"}, 5, g));
	}

	@Test
	public void testMetaPathWalkPartialSum2() {
		GraphData g = GraphData.getGraph2();
		List <Row> walks = new MetaPathWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(false)
			.setMetaPath("abca")
			.setVertexCol("node")
			.setWeightCol("weight")
			.setSamplingMethod("PARTIALSUM")
			.setTypeCol("type")
			.linkFrom(g.getGraphOp(), g.getNodeAndTypesOp()).collect();
		assertEquals(4, walks.size());
		assertTrue(GraphDataUtils.checkHeteWalksValid(walks, new String[]{"abca"}, 5, g));
	}
}