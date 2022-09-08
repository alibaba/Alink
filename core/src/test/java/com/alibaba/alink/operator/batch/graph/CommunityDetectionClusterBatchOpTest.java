package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class CommunityDetectionClusterBatchOpTest extends AlinkTestBase {
	@Test
	public void testIntWithVertex() throws Exception {
		Row[] edges = new Row[]{
			Row.of(1, 2, 0.7F),
			Row.of(1, 3, 0.7F),
			Row.of(1, 4, 0.6F),
			Row.of(2, 3, 0.7F),
			Row.of(2, 4, 0.6F),
			Row.of(3, 4, 0.6F),
			Row.of(4, 6, 0.3F),
			Row.of(5, 6, 0.6F),
			Row.of(5, 7, 0.7F),
			Row.of(5, 8, 0.7F),
			Row.of(6, 7, 0.6F),
			Row.of(6, 8, 0.6F),
			Row.of(7, 8, 0.7F)
		};
		Row[] vertexs = new Row[]{
			Row.of(1, 0.7),
			Row.of(2, 0.7),
			Row.of(3, 0.7),
			Row.of(4, 0.5),
			Row.of(5, 0.7),
			Row.of(6, 0.5),
			Row.of(7, 0.7),
			Row.of(8, 0.7)
		};
		BatchOperator vertexsOp = new MemSourceBatchOp(vertexs, new String[]{"vertexCol", "weight"});
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").setEdgeWeightCol("weight")
			.setVertexCol("vertexCol").setVertexWeightCol("weight")
			.linkFrom(edgeData, vertexsOp);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)), String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
		Assert.assertNotEquals(result.get("1"), result.get("5"));
	}

	@Test
	public void testIntWithNotAllVertex() throws Exception {
		Row[] edges = new Row[]{
			Row.of(1, 2, 0.7),
			Row.of(1, 3, 0.7),
			Row.of(1, 4, 0.6),
			Row.of(2, 3, 0.7),
			Row.of(2, 4, 0.6),
			Row.of(3, 4, 0.6),
			Row.of(4, 6, 0.3),
			Row.of(5, 6, 0.6),
			Row.of(5, 7, 0.7),
			Row.of(5, 8, 0.7),
			Row.of(6, 7, 0.6),
			Row.of(6, 8, 0.6),
			Row.of(7, 8, 0.7)
		};
		Row[] vertexs = new Row[]{
			Row.of(1, 0.7),
			Row.of(2, 0.7),
			Row.of(3, 0.7),
			Row.of(4, 0.5),
			//Row.of(5, 0.7),
			Row.of(6, 0.5),
			Row.of(7, 0.7),
			Row.of(8, 0.7)
		};
		BatchOperator edgesOp = new MemSourceBatchOp(edges, new String[]{"fromCol", "toCol", "weight"});
		BatchOperator vertexsOp = new MemSourceBatchOp(vertexs, new String[]{"vertexCol", "weight"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("fromCol")
			.setEdgeTargetCol("toCol")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertexCol")
			.setVertexWeightCol("weight")
			.linkFrom(edgesOp, vertexsOp);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)), String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
		Assert.assertNotEquals(result.get("1"), result.get("5"));
	}

	@Test
	public void testStringWithVertex() throws Exception {
		Row[] edges = new Row[]{
			Row.of("1", "2", 0.7),
			Row.of("1", "3", 0.7),
			Row.of("1", "4", 0.6),
			Row.of("2", "3", 0.7),
			Row.of("2", "4", 0.6),
			Row.of("3", "4", 0.6),
			Row.of("4", "6", 0.3),
			Row.of("5", "6", 0.6),
			Row.of("5", "7", 0.7),
			Row.of("5", "8", 0.7),
			Row.of("6", "7", 0.6),
			Row.of("6", "8", 0.6),
			Row.of("7", "8", 0.7)
		};
		Row[] vertexs = new Row[]{
			Row.of("1", 0.7),
			Row.of("2", 0.7),
			Row.of("3", 0.7),
			Row.of("4", 0.5),
			Row.of("5", 0.7),
			Row.of("6", 0.5),
			Row.of("7", 0.7),
			Row.of("8", 0.7)
		};
		BatchOperator vertexsOp = new MemSourceBatchOp(vertexs, new String[]{"vertexCol", "weight"});
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").setEdgeWeightCol("weight")
			.setVertexCol("vertexCol").setVertexWeightCol("weight");

		List <Row> listRes = op.linkFrom(edgeData, vertexsOp).collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put((String) r.getField(0),String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
		Assert.assertNotEquals(result.get("1"), result.get("5"));
	}

	@Test
	public void testLong() throws Exception {
		Row[] edges = new Row[] {
			Row.of(3L, 1L, 1.0),
			Row.of(3L, 0L, 1.0),
			Row.of(0L, 1L, 1.0),
			Row.of(0L, 2L, 1.0),
			Row.of(2L, 1L, 1.0),
			Row.of(2L, 4L, 0.1),
			Row.of(5L, 4L, 1.0),
			Row.of(7L, 4L, 1.0),
			Row.of(5L, 6L, 1.0),
			Row.of(5L, 8L, 1.0),
			Row.of(5L, 7L, 1.0),
			Row.of(7L, 8L, 1.0),
			Row.of(6L, 8L, 1.0),
			Row.of(12L, 10L, 1.0),
			Row.of(12L, 11L, 1.0),
			Row.of(12L, 13L, 1.0),
			Row.of(12L, 9L, 1.0),
			Row.of(10L, 9L, 1.0),
			Row.of(8L, 9L, 0.1),
			Row.of(13L, 9L, 1.0),
			Row.of(10L, 7L, 0.1),
			Row.of(10L, 11L, 1.0),
			Row.of(11L, 13L, 1.0)
		};
		Row[] vertices = new Row[] {
			Row.of(0L),
			Row.of(1L),
			Row.of(2L),
			Row.of(3L),
			Row.of(4L),
			Row.of(5L),
			Row.of(6L),
			Row.of(7L),
			Row.of(8L),
			Row.of(9L),
			Row.of(10L),
			Row.of(11L),
			Row.of(12L),
			Row.of(13L),
			Row.of(14L),
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertex"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").setEdgeWeightCol("weight")
			.setVertexCol("vertex");
		op.linkFrom(edgeData, vertexData).print();
	}

	@Test
	public void testIntNoVertex() throws Exception {
		Row[] edges = new Row[]{
			Row.of(1, 2, 0.7),
			Row.of(1, 3, 0.7),
			Row.of(1, 4, 0.6),
			Row.of(2, 3, 0.7),
			Row.of(2, 4, 0.6),
			Row.of(3, 4, 0.6),
			Row.of(4, 6, 0.3),
			Row.of(5, 6, 0.6),
			Row.of(5, 7, 0.7),
			Row.of(5, 8, 0.7),
			Row.of(6, 7, 0.6),
			Row.of(6, 8, 0.6),
			Row.of(7, 8, 0.7)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").setEdgeWeightCol("weight");
		op.linkFrom(edgeData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)), String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
		Assert.assertNotEquals(result.get("1"), result.get("5"));
	}

	@Test
	public void testIntNoWeight() throws Exception {
		Row[] edges = new Row[]{
			Row.of(1, 2),
			Row.of(1, 3),
			Row.of(1, 4),
			Row.of(2, 3),
			Row.of(2, 4),
			Row.of(3, 4),
			Row.of(4, 6),
			Row.of(5, 6),
			Row.of(5, 7),
			Row.of(5, 8),
			Row.of(6, 7),
			Row.of(6, 8),
			Row.of(7, 8)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(edgeData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)), String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
	}

	@Test
	public void testStringNoVertex() throws Exception {
		Row[] edges = new Row[]{
			Row.of("1", "2", 0.7),
			Row.of("1", "3", 0.7),
			Row.of("1", "4", 0.6),
			Row.of("2", "3", 0.7),
			Row.of("2", "4", 0.6),
			Row.of("3", "4", 0.6),
			Row.of("4", "6", 0.3),
			Row.of("5", "6", 0.6),
			Row.of("5", "7", 0.7),
			Row.of("5", "8", 0.7),
			Row.of("6", "7", 0.6),
			Row.of("6", "8", 0.6),
			Row.of("7", "8", 0.7)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").setEdgeWeightCol("weight");
		op.linkFrom(edgeData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)), String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
		Assert.assertNotEquals(result.get("1"), result.get("5"));
	}

	@Test
	public void testStringNoWeight() throws Exception {
		Row[] edges = new Row[]{
			Row.of("1", "2"),
			Row.of("1", "3"),
			Row.of("1", "4"),
			Row.of("2", "3"),
			Row.of("2", "4"),
			Row.of("3", "4"),
			Row.of("4", "6"),
			Row.of("5", "6"),
			Row.of("5", "7"),
			Row.of("5", "8"),
			Row.of("6", "7"),
			Row.of("6", "8"),
			Row.of("7", "8")
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target"});
		CommunityDetectionClusterBatchOp op = new CommunityDetectionClusterBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(edgeData);

		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)), String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), result.get("2"));
		Assert.assertEquals(result.get("1"), result.get("3"));
		Assert.assertEquals(result.get("1"), result.get("4"));
		Assert.assertEquals(result.get("5"), result.get("6"));
		Assert.assertEquals(result.get("5"), result.get("7"));
		Assert.assertEquals(result.get("5"), result.get("8"));
	}
}