package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ModularityCalBatchOpTest extends AlinkTestBase {

	@Test
	public void testModularity() throws Exception {

		List <Row> edges = Arrays.asList(
			Row.of("3L", "1L", 1.0),
			Row.of("3L", "0L", 1.0),
			Row.of("0L", "1L", 1.0),
			Row.of("0L", "2L", 1.0),
			Row.of("2L", "1L", 1.0),
			Row.of("2L", "4L", 1.0),
			Row.of("5L", "4L", 1.0),
			Row.of("7L", "4L", 1.0),
			Row.of("5L", "6L", 1.0),
			Row.of("5L", "8L", 1.0),
			Row.of("5L", "7L", 1.0),
			Row.of("7L", "8L", 1.0),
			Row.of("6L", "8L", 1.0),
			Row.of("12L", "10L", 1.0),
			Row.of("12L", "11L", 1.0),
			Row.of("12L", "13L", 1.0),
			Row.of("12L", "9L", 1.0),
			Row.of("10L", "9L", 1.0),
			Row.of("8L", "9L", 1.0),
			Row.of("13L", "9L", 1.0),
			Row.of("10L", "7L", 1.0),
			Row.of("10L", "11L", 1.0),
			Row.of("11L", "13L", 1.0)
		);
		List <Row> vertices = Arrays.asList(
			Row.of("0L", 1L),
			Row.of("1L", 1L),
			Row.of("2L", 1L),
			Row.of("3L", 1L),
			Row.of("4L", 2L),
			Row.of("5L", 2L),
			Row.of("6L", 2L),
			Row.of("7L", 2L),
			Row.of("8L", 2L),
			Row.of("9L", 3L),
			Row.of("10L", 3L),
			Row.of("11L", 3L),
			Row.of("12L", 3L),
			Row.of("13L", 3L)
		);

		BatchOperator edgeData = new MemSourceBatchOp(edges, "source string,target string,weight double");

		BatchOperator vertexData = new MemSourceBatchOp(vertices, "vertex string,community long");

		double modularity = (double) new ModularityCalBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setVertexCol("vertex")
			.setAsUndirectedGraph(false)
			.setVertexCommunityCol("community")
			.linkFrom(edgeData, vertexData).collect().get(0).getField(0);
		Assert.assertEquals(modularity, 0.5274, 0.0001);
	}
}