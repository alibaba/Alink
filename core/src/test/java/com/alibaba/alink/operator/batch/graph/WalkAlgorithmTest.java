package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.dataproc.SampleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WalkAlgorithmTest extends AlinkTestBase {

	@Test
	public void Test() throws Exception {
		MemSourceBatchOp edge = new MemSourceBatchOp(
			new Object[][] {
				{0L, 6L, 3L},
				{2L, 3L, 6L},
				{3L, 2L, 1L},
				{1L, 3L, 6L},
				{1L, 4L, 1L},
				{3L, 2L, 1L},
				{2L, 1L, 6L},
				{8L, 4L, 1L},
				{1L, 2L, 3L},
				{2L, 3L, 6L},
				{2L, 0L, 1L},
				{1L, 0L, 6L},
				{1L, 9L, 1L},
				{3L, 8L, 1L},
				{2L, 7L, 6L},
				{2L, 6L, 1L},
				{1L, 5L, 5L}
			},
			new String[] {"source", "target", "value"});

		MemSourceBatchOp node = new MemSourceBatchOp(
			new Object[][] {
				{1L, "A"},
				{2L, "A"},
				{3L, "A"},
				{4L, "A"},
				{0L, "A"},
				{5L, "B"},
				{6L, "B"},
				{7L, "B"},
				{8L, "B"},
				{9L, "B"},
				{10L, "B"}
			},
			new String[] {"vertex", "type"});

		new MetaPathWalkBatchOp(new Params()
			.set("walkNum", 3)
			.set("walkLength", 10)
			//.set("weightColName", "value")
			.set("isToUndigraph", false)
			.set("metaPath", "ABA")
			.set("delimiter", ",")
			.set("vertexColName", "vertex")
			.set("typeColName", "type"))
			.setSourceCol("source")
			.setTargetCol("target").linkFrom(edge, node).getDataSet().collect();
	}

	@Test
	public void Test1() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"start", "end", "value"},
			new TypeInformation <?>[] {org.apache.flink.table.api.Types.STRING(),
				org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.DOUBLE()}
		);
		List <Row> rows = new ArrayList <>();
		for (int i = 1; i < 2; ++i) {
			for (int j = 1; j < 3000; ++j) {
				double weight = j > 195 ? 10.0 : 1.0;
				rows.add(Row.of("f" + i, "f" + j, weight));
			}
		}

		MemSourceBatchOp edge = new MemSourceBatchOp(rows, schema);

		TableSchema nschema = new TableSchema(
			new String[] {"node", "type"},
			new TypeInformation <?>[] {org.apache.flink.table.api.Types.STRING(),
				org.apache.flink.table.api.Types.STRING()}
		);
		List <Row> nrows = new ArrayList <>();

		for (int j = 1; j < 3000; ++j) {
			String weight = j % 2 == 1 ? "A" : "B";
			nrows.add(Row.of("f" + j, weight));
		}

		MemSourceBatchOp node = new MemSourceBatchOp(nrows, nschema);
		new MetaPathWalkBatchOp(new Params()
			.set("walkNum", 1)
			.set("walkLength", 2)
			//.set("weightColName", "value")
			.set("isToUndigraph", true)
			.set("metaPath", "ABA")
			.set("delimiter", ",")
			.set("vertexColName", "node")
			.set("typeColName", "type"))
			.setSourceCol("start")
			.setTargetCol("end").linkFrom(edge, node).link(new SampleBatchOp(0.000001)).collect();

		new RandomWalkBatchOp()
			.setWalkNum(1)
			.setWalkLength(5)
			.setWeightCol("value")
			.setIsToUndigraph(true)
			.setIsWeightedSampling(true)
			.setSourceCol("start")
			.setTargetCol("end").linkFrom(edge).link(new SampleBatchOp(0.000001)).collect();

		new RandomWalkBatchOp()
			.setWalkNum(1)
			.setWalkLength(5)
			.setWeightCol("value")
			.setIsToUndigraph(false)
			.setSourceCol("start")
			.setIsWeightedSampling(false)
			.setTargetCol("end").linkFrom(edge).link(new SampleBatchOp(0.000001)).collect();
	}
}
