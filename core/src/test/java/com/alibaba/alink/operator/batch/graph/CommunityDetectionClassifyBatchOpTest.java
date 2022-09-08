package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class CommunityDetectionClassifyBatchOpTest extends AlinkTestBase {

	@Test
	public void testPaiData() throws Exception {
		Row[] edges = new Row[] {
			Row.of("1L", "2L", 0.2),
			Row.of("1L", "3L", 0.8),
			Row.of("2L", "3L", 1.0),
			Row.of("4L", "2L", 1.0),
			Row.of("5L", "4L", 1.0),
		};
		Row[] vertices = new Row[] {
			Row.of("1L", "X", 1.0),
			Row.of("4L", "Y", 1.0)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels", "weight"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.setVertexWeightCol("weight")
			.setAsUndirectedGraph(true)
			.linkFrom(edgeData, vertexData);
		op.print();
	}

	@Test
	public void testPaiDataLong() throws Exception {
		Row[] edges = new Row[] {
			Row.of(1L, 2L, 0.2),
			Row.of(1L, 3L, 0.8),
			Row.of(2L, 3L, 1.0),
			Row.of(4L, 2L, 1.0)
		};
		Row[] vertices = new Row[] {
			Row.of(1L, "X", 1.0),
			Row.of(4L, "Y", 1.0)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels", "weight"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.setVertexWeightCol("weight")
			.setAsUndirectedGraph(false)
			.linkFrom(edgeData, vertexData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)),String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), "X");
		Assert.assertEquals(result.get("2"), "Y");
		Assert.assertEquals(result.get("3"), "X");
		Assert.assertEquals(result.get("4"), "Y");
	}

	@Test
	public void testPaiDataInt() throws Exception {
		Row[] edges = new Row[] {
			Row.of(1, 2, 0.2),
			Row.of(1, 3, 0.8),
			Row.of(2, 3, 1.0),
			Row.of(4, 2, 1.0)
		};
		Row[] vertices = new Row[] {
			Row.of(1, "X", 1.0),
			Row.of(4, "Y", 1.0)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels", "weight"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.setVertexWeightCol("weight")
			.linkFrom(edgeData, vertexData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)),String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), "X");
		Assert.assertEquals(result.get("2"), "Y");
		Assert.assertEquals(result.get("3"), "X");
		Assert.assertEquals(result.get("4"), "Y");
	}

	@Test
	public void testPaiDataIntLabel() throws Exception {
		Row[] edges = new Row[] {
			Row.of(1, 2, 0.2),
			Row.of(1, 3, 0.8),
			Row.of(2, 3, 1.0),
			Row.of(4, 2, 1.0)
		};
		Row[] vertices = new Row[] {
			Row.of(1, 1, 1.0),
			Row.of(4, 2, 1.0)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels", "weight"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.setVertexWeightCol("weight")
			.linkFrom(edgeData, vertexData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)),String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), "1");
		Assert.assertEquals(result.get("2"), "2");
		Assert.assertEquals(result.get("3"), "1");
		Assert.assertEquals(result.get("4"), "2");
	}

	@Test
	public void testPaiDataNoWeight() throws Exception {
		Row[] edges = new Row[] {
			Row.of("1L", "2L"),
			Row.of("1L", "3L"),
			Row.of("2L", "3L"),
			Row.of("4L", "2L")
		};
		Row[] vertices = new Row[] {
			Row.of("1L", "X"),
			Row.of("4L", "Y")
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.linkFrom(edgeData, vertexData);
		op.print();
	}

	@Test
	public void testPaiDataIntLabelNoWeight() throws Exception {
		Row[] edges = new Row[] {
			Row.of(1, 2),
			Row.of(1, 3),
			Row.of(2, 3),
			Row.of(4, 2)
		};
		Row[] vertices = new Row[] {
			Row.of(1, 1),
			Row.of(4, 2)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.linkFrom(edgeData, vertexData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)),String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), "1");
		Assert.assertEquals(result.get("2"), "2");
		Assert.assertEquals(result.get("3"), "1");
		Assert.assertEquals(result.get("4"), "2");
	}

	@Test
	public void testPaiMissPartLabel() throws Exception {
		Row[] edges = new Row[] {
			Row.of(1, 2, 0.2),
			Row.of(1, 3, 0.8),
			Row.of(2, 3, 1.0),
			Row.of(4, 2, 1.0),
			Row.of(5, 6, 1.0)
		};
		Row[] vertices = new Row[] {
			Row.of(1, 1, 1.0),
			Row.of(4, 2, 1.0)
		};
		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});
		BatchOperator vertexData = new MemSourceBatchOp(vertices, new String[] {"vertices", "labels", "weight"});

		CommunityDetectionClassifyBatchOp op = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertices")
			.setVertexLabelCol("labels")
			.setVertexWeightCol("weight")
			.linkFrom(edgeData, vertexData);
		List <Row> listRes = op.collect();
		HashMap <String, String> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(String.valueOf(r.getField(0)),String.valueOf(r.getField(1)));
		}
		Assert.assertEquals(result.get("1"), "1");
		Assert.assertEquals(result.get("2"), "2");
		Assert.assertEquals(result.get("3"), "1");
		Assert.assertEquals(result.get("4"), "2");
	}

}