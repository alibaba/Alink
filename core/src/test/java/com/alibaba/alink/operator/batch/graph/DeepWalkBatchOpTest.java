package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DeepWalkBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"start", "end", "value"},
			new TypeInformation <?>[] {Types.STRING(), Types.STRING(), Types.DOUBLE()}
		);
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("Alice", "Lisa", 1.));
		rows.add(Row.of("Lisa", "Alice", 1.));
		rows.add(Row.of("Lisa", "Karry", 1.));
		rows.add(Row.of("Karry", "Lisa", 1.));
		rows.add(Row.of("Karry", "Bella", 1.));
		rows.add(Row.of("Bella", "Karry", 1.));
		rows.add(Row.of("Bella", "Lucy", 1.));
		rows.add(Row.of("Lucy", "Bella", 1.));
		rows.add(Row.of("Lucy", "Bob", 1.));
		rows.add(Row.of("Bob", "Lucy", 1.));

		MemSourceBatchOp source = new MemSourceBatchOp(rows, schema);

		DeepWalkBatchOp deepWalkBatchOp = new DeepWalkBatchOp(new Params())
			.setSourceCol("start")
			.setTargetCol("end")
			.setWeightCol("value")
			.setWalkLength(10)
			.setWalkNum(10)
			.setVectorSize(3)
			.setWindow(3)
			.setNegative(3)
			.setNumIter(2);
		Assert.assertEquals(6, deepWalkBatchOp.linkFrom(source).collect().size());
	}
}
