package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestRandomWalk extends AlinkTestBase {

	public static void main(String[] args) throws Exception {

		TableSchema schema = new TableSchema(
			new String[] {"start", "end", "value"},
			new TypeInformation <?>[] {org.apache.flink.table.api.Types.STRING(),
				org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.DOUBLE()}
		);
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("Alice", "Lisa", 1.));
		//rows.add(Row.of("Lisa", "Alice", 1.));
		rows.add(Row.of("Lisa", "Karry", 1.));
		//rows.add(Row.of("Karry", "Lisa", 1.));
		rows.add(Row.of("Karry", "Bella", 1.));
		//rows.add(Row.of("Bella", "Karry", 1.));
		rows.add(Row.of("Bella", "Lucy", 1.));
		//rows.add(Row.of("Lucy", "Bella", 1.));
		rows.add(Row.of("Lucy", "Bob", 1.));
		//rows.add(Row.of("Bob", "Lucy", 1.));
		rows.add(Row.of("John", "Bob", 1.));
		//rows.add(Row.of("Bob", "John", 1.));
		rows.add(Row.of("John", "Stella", 1.));
		rows.add(Row.of("John", "Kate", 1.));
		//rows.add(Row.of("Stella", "John", 1.));
		rows.add(Row.of("Kate", "Stella", 1.));
		//rows.add(Row.of("Stella", "Kate", 1.));
		rows.add(Row.of("Kate", "Jack", 1.));
		//rows.add(Row.of("Jack", "Kate", 1.));
		rows.add(Row.of("Jess", "Jack", 1.));
		//rows.add(Row.of("Jack", "Jess", 1.));
		rows.add(Row.of("Jess", "Jacob", 1.));
		//rows.add(Row.of("Jacob", "Jess", 1.));

		MemSourceBatchOp source = new MemSourceBatchOp(rows, schema);

		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(2)
			.setWalkLength(5)
			.setIsToUndigraph(false)
			.setSourceCol("start")
			.setTargetCol("end")
			.setWeightCol("value");
		randomWalkBatchOp.linkFrom(source).collect();
	}

	@Test
	public void Test() throws Exception {
		MemSourceBatchOp source = new MemSourceBatchOp(
			new Object[][] {
				{1L, 3L, 6L},
				{1L, 2L, 1020L},
				{2L, 3L, 3L},
				{1L, 4L, 5L}
			},
			new String[] {"source", "target", "value"});
		RandomWalkBatchOp randomWalkBatchOp = new RandomWalkBatchOp()
			.setWalkNum(10)
			.setWalkLength(5)
			.setSourceCol("source")
			.setTargetCol("target")

			.setIsToUndigraph(false)
			.setIsWeightedSampling(true)
			.setWeightCol("value");
		randomWalkBatchOp.linkFrom(source).collect();

	}

}
