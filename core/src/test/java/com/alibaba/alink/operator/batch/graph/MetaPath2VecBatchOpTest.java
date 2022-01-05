package com.alibaba.alink.operator.batch.graph;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class MetaPath2VecBatchOpTest extends AlinkTestBase {
	@Test
	public void Test() throws Exception {
		Long newEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();
		MemSourceBatchOp edge = new MemSourceBatchOp(
			new Object[][] {
				{1L, 2L, 3L},
				{2L, 3L, 6L},
				{3L, 2L, 1L},
				{1L, 3L, 6L},
				{1L, 4L, 1L},
				{3L, 2L, 1L},
				{2L, 1L, 6L},
				{2L, 4L, 1L},
				{1L, 2L, 3L},
				{2L, 3L, 6L},
				{3L, 2L, 1L},
				{1L, 3L, 6L},
				{1L, 4L, 1L},
				{3L, 2L, 1L},
				{2L, 1L, 6L},
				{2L, 4L, 1L},
				{1L, 4L, 5L}
			},
			new String[] {"source", "target", "value"})
			.setMLEnvironmentId(newEnvId);

		MemSourceBatchOp node = new MemSourceBatchOp(
			new Object[][] {
				{1L, "A"},
				{2L, "A"},
				{3L, "B"},
				{4L, "B"}
			},
			new String[] {"vertex", "type"})
			.setMLEnvironmentId(newEnvId);

		MetaPath2VecBatchOp metaPath2VecBatchOp = new MetaPath2VecBatchOp()
			.setWalkNum(10)
			.setWalkLength(10)
			.setIsToUndigraph(true)
			.setMetaPath("ABA, BAB")
			.setVertexCol("vertex")
			.setTypeCol("type")
			.setWeightCol("value")
			.setSourceCol("source")
			.setTargetCol("target")
			.setMLEnvironmentId(newEnvId);
		Assert.assertEquals(4, metaPath2VecBatchOp.linkFrom(edge, node).collect().size());
	}

	@Test
	public void Test1() throws Exception {
		MemSourceBatchOp edge = new MemSourceBatchOp(
			new Object[][] {
				{1L, 2L, 3L},
				{2L, 3L, 6L},
				{3L, 2L, 1L},
				{1L, 3L, 6L},
				{1L, 4L, 1L},
				{3L, 2L, 1L},
				{2L, 1L, 6L},
				{2L, 4L, 1L},
				{1L, 2L, 3L},
				{2L, 3L, 6L},
				{3L, 2L, 1L},
				{1L, 3L, 6L},
				{1L, 4L, 1L},
				{3L, 2L, 1L},
				{2L, 1L, 6L},
				{2L, 4L, 1L},
				{1L, 4L, 5L}
			},
			new String[] {"source", "target", "value"});

		MemSourceBatchOp node = new MemSourceBatchOp(
			new Object[][] {
				{1L, "A"},
				{2L, "A"},
				{3L, "B"},
				{4L, "B"}
			},
			new String[] {"vertex", "type"});

		MetaPath2VecBatchOp metaPath2VecBatchOp = new MetaPath2VecBatchOp()
			.setWalkNum(10)
			.setWalkLength(10)
			.setIsToUndigraph(true)
			.setMetaPath("ABA, BAB")
			.setVertexCol("vertex")
			.setTypeCol("type")
			.setMode("metapath2vecpp")
			.setWeightCol("value")
			.setSourceCol("source")
			.setTargetCol("target");

		Assert.assertEquals(4, metaPath2VecBatchOp.linkFrom(edge, node).collect().size());
	}

}