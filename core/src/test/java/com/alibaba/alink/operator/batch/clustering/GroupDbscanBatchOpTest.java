package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.params.clustering.HasLatitudeCol;
import com.alibaba.alink.params.clustering.HasLongitudeCol;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Test
 */
public class GroupDbscanBatchOpTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static Row[] array3 = new Row[] {
		Row.of(1, "id_1", 103.829686, 30.360428),
		Row.of(1, "id_2", 103.894723, 30.398725),
		Row.of(1, "id_3", 103.847446, 30.338719),
		Row.of(1, "id_4", 103.828968, 30.359439),
		Row.of(1, "id_5", 103.828968, 30.359439),
		Row.of(1, "id_6", 103.828968, 30.359439),
		Row.of(1, "id_7", 103.828968, 30.359439),
		Row.of(1, "id_8", 103.828968, 30.359439),
		Row.of(1, "id_9", 103.828968, 30.359439),
		Row.of(1, "id_10", 103.828968, 30.359439),
		Row.of(1, "id_11", 103.828968, 30.359439),
		Row.of(1, "id_12", 103.828968, 30.359439),
		Row.of(1, "id_13", 103.828968, 30.359439),
		Row.of(1, "id_14", 103.828968, 30.359439),
		Row.of(1, "id_15", 103.828968, 30.359439),
		Row.of(1, "id_16", 103.828968, 30.359439),
		Row.of(1, "id_17", 103.891378, 30.398637),
		Row.of(1, "id_18", 103.806528, 30.35827),
		Row.of(1, "id_19", 103.919133, 30.336444),
		Row.of(1, "id_20", 103.879987, 30.476604),
		Row.of(1, "id_21", 103.847939, 30.335884),
		Row.of(1, "id_22", 103.885921, 30.358769)

	};

	@Test
	public void linkFrom() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

		HashMap <String, Tuple2 <String, Long>> map = new HashMap <>();
		map.put("id_5", Tuple2.of("CORE", 0L));
		map.put("id_6", Tuple2.of("CORE", 0L));
		map.put("id_7", Tuple2.of("CORE", 0L));
		map.put("id_8", Tuple2.of("CORE", 0L));
		map.put("id_9", Tuple2.of("CORE", 0L));
		map.put("id_10", Tuple2.of("CORE", 0L));
		map.put("id_11", Tuple2.of("CORE", 0L));
		map.put("id_12", Tuple2.of("CORE", 0L));
		map.put("id_13", Tuple2.of("CORE", 0L));
		map.put("id_14", Tuple2.of("CORE", 0L));
		map.put("id_15", Tuple2.of("CORE", 0L));
		map.put("id_16", Tuple2.of("CORE", 0L));
		map.put("id_17", Tuple2.of("NOISE", -2147483648L));
		map.put("id_18", Tuple2.of("NOISE", -2147483648L));
		map.put("id_19", Tuple2.of("NOISE", -2147483648L));
		map.put("id_20", Tuple2.of("NOISE", -2147483648L));
		map.put("id_21", Tuple2.of("NOISE", -2147483648L));
		map.put("id_22", Tuple2.of("NOISE", -2147483648L));
		map.put("id_1", Tuple2.of("CORE", 0L));
		map.put("id_2", Tuple2.of("NOISE", -2147483648L));
		map.put("id_3", Tuple2.of("NOISE", -2147483648L));
		map.put("id_4", Tuple2.of("CORE", 0L));

		GroupDbscanBatchOp op = new GroupDbscanBatchOp(new Params().set(HasLatitudeCol.LATITUDE_COL, "c1")
			.set(HasLongitudeCol.LONGITUDE_COL, "c2"))
			.setIdCol("id")
			.setGroupCols("group")
			.setMinPoints(4)
			.setEpsilon(0.6)
			.setIsOutputVector(true)
			.setDistanceType(DistanceType.HAVERSINE.name());

		List <Row> res = op.linkFrom(data).select(new String[] {"id", "type", "cluster_id"}).getDataSet().collect();

		for (Row re : res) {
			String id = (String) re.getField(0);
			Tuple2 <String, Long> t = map.get(id);
			Assert.assertEquals(t.f0, re.getField(1));
			Assert.assertEquals(t.f1, re.getField(2));
		}
	}

	@Test
	public void testMaxSample() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

		GroupDbscanBatchOp op = new GroupDbscanBatchOp()
			.setFeatureCols("c1", "c2")
			.setIdCol("id")
			.setGroupCols("group")
			.setGroupMaxSamples(10)
			.setMinPoints(2)
			.setEpsilon(0.6)
			.linkFrom(data);

		Assert.assertEquals(op.count(), 22);
	}

	@Test
	public void testException1() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("featureColNames should be set !");
		GroupDbscanBatchOp op = new GroupDbscanBatchOp()
			.setFeatureCols()
			.setIdCol("group")
			.setGroupCols("group")
			.setGroupMaxSamples(10)
			.setMinPoints(2)
			.setEpsilon(0.6)
			.linkFrom(data);
		op.print();
	}

	@Test
	public void testException2() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("idCol column should NOT be included in featureColNames !");
		GroupDbscanBatchOp op = new GroupDbscanBatchOp()
			.setFeatureCols("c1", "c2")
			.setIdCol("c1")
			.setGroupCols("group")
			.setGroupMaxSamples(10)
			.setMinPoints(2)
			.setEpsilon(0.6)
			.linkFrom(data);
		op.print();
	}

	@Test
	public void testException3() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("idCol column should NOT be included in groupColNames ");
		GroupDbscanBatchOp op = new GroupDbscanBatchOp()
			.setFeatureCols("c1", "c2")
			.setIdCol("group")
			.setGroupCols("group")
			.setGroupMaxSamples(10)
			.setMinPoints(2)
			.setEpsilon(0.6)
			.linkFrom(data);
		op.print();
	}

	@Test
	public void testException4() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("groupColNames should NOT be included in featureColNames!");
		GroupDbscanBatchOp op = new GroupDbscanBatchOp()
			.setFeatureCols("group", "c1", "c2")
			.setIdCol("id")
			.setGroupCols("group")
			.setGroupMaxSamples(10)
			.setMinPoints(2)
			.setEpsilon(0.6)
			.linkFrom(data);
		op.print();
	}

}