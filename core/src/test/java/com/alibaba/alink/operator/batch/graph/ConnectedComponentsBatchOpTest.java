package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class ConnectedComponentsBatchOpTest extends AlinkTestBase {
	@Test
	public void testUnStable() throws Exception {
		Row[] edgeRows = new Row[] {
			Row.of(1, 2),
			Row.of(2, 3),
			Row.of(3, 4),
			Row.of(4, 5),
			Row.of(6, 7),
			Row.of(7, 8),
			Row.of(8, 9),
			Row.of(9, 6),
			Row.of(10, 11),
			Row.of(10, 12),
			Row.of(12, 13),
		};
		BatchOperator edgeData = new MemSourceBatchOp(edgeRows, new String[] {"source", "target"});

		BatchOperator res = new ConnectedComponentsBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(edgeData);

		List<Row> listRes = res.collect();
		HashMap <Object, Object> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(r.getField(0), r.getField(1));
		}
		Assert.assertEquals(result.get(1), result.get(2));
		Assert.assertEquals(result.get(1), result.get(3));
		Assert.assertEquals(result.get(1), result.get(4));
		Assert.assertEquals(result.get(1), result.get(5));
		Assert.assertEquals(result.get(6), result.get(7));
		Assert.assertEquals(result.get(6), result.get(8));
		Assert.assertEquals(result.get(6), result.get(9));
		Assert.assertEquals(result.get(10), result.get(11));
		Assert.assertEquals(result.get(10), result.get(12));
		Assert.assertEquals(result.get(10), result.get(13));
	}

	@Test
	public void testStable() throws Exception {
		Row[] edgeRows = new Row[] {
			Row.of(1, 2),
			Row.of(2, 3),
			Row.of(3, 4),
			Row.of(4, 5),
			Row.of(6, 7),
			Row.of(7, 8),
			Row.of(8, 9),
			Row.of(9, 6),
			Row.of(10, 11),
			Row.of(10, 12),
			Row.of(12, 13),
		};
		BatchOperator edgeData = new MemSourceBatchOp(edgeRows, new String[] {"source", "target"});

		BatchOperator res = new ConnectedComponentsBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(edgeData);

		res.lazyPrint(-1);
		List<Row> listRes = res.collect();
		HashMap <Object, Object> result = new HashMap <>();
		for (Row r : listRes) {
			result.put(r.getField(0), r.getField(1));
		}
		Assert.assertEquals(result.get(1), result.get(2));
		Assert.assertEquals(result.get(1), result.get(3));
		Assert.assertEquals(result.get(1), result.get(4));
		Assert.assertEquals(result.get(1), result.get(5));
		Assert.assertEquals(result.get(6), result.get(7));
		Assert.assertEquals(result.get(6), result.get(8));
		Assert.assertEquals(result.get(6), result.get(9));
		Assert.assertEquals(result.get(10), result.get(11));
		Assert.assertEquals(result.get(10), result.get(12));
		Assert.assertEquals(result.get(10), result.get(13));
		Assert.assertEquals(result.get(1), 0L);
		Assert.assertEquals(result.get(10), 1L);
		Assert.assertEquals(result.get(6), 9L);
	}
}