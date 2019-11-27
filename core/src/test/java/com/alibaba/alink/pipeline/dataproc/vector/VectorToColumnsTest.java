package com.alibaba.alink.pipeline.dataproc.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VectorToColumnsTest {
	AlgoOperator getData(boolean isBatch) {
		TableSchema schema = new TableSchema(
			new String[] {"id", "c0", "c1", "c2"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.STRING}
		);

		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(new Object[] {"0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0", "1 4 6 8"}));
		rows.add(Row.of(new Object[] {"1", "$6$1:2.0 2:3.0 5:4.3", "2.0 3.0 2.0", "1 4 6 8"}));

		if (isBatch) {
			return new MemSourceBatchOp(rows, schema);
		} else {
			return new MemSourceStreamOp(rows, schema);
		}
	}

	@Test
	public void pipelineBatchTest() throws Exception {
		BatchOperator res = new VectorToColumns().setSelectedCol("c0").setOutputCols(
			new String[] {"f0", "f1", "f2", "f3", "f4", "f5"}).transform((BatchOperator)getData(true));
		List rows = res.getDataSet().collect();
		HashMap<String, Tuple6<Double, Double, Double, Double, Double, Double>> map = new HashMap<String, Tuple6<Double, Double, Double, Double, Double, Double>>();
		map.put((String) ((Row) rows.get(0)).getField(0), Tuple6.of(
			(Double) ((Row) rows.get(0)).getField(4),
			(Double) ((Row) rows.get(0)).getField(5),
			(Double) ((Row) rows.get(0)).getField(6),
			(Double) ((Row) rows.get(0)).getField(7),
			(Double) ((Row) rows.get(0)).getField(8),
			(Double) ((Row) rows.get(0)).getField(9)));
		map.put((String) ((Row) rows.get(1)).getField(0), Tuple6.of(
			(Double) ((Row) rows.get(1)).getField(4),
			(Double) ((Row) rows.get(1)).getField(5),
			(Double) ((Row) rows.get(1)).getField(6),
			(Double) ((Row) rows.get(1)).getField(7),
			(Double) ((Row) rows.get(1)).getField(8),
			(Double) ((Row) rows.get(1)).getField(9)));

		assertEquals(map.get("0"), new Tuple6<>(0.0, 2.0, 3.0, 0.0, 0.0, 4.3));
		assertEquals(map.get("1"), new Tuple6<>(0.0, 2.0, 3.0, 0.0, 0.0, 4.3));
	}

	@Test
	public void pipelineStreamTest() throws Exception {
		new VectorToColumns().setSelectedCol("c0").setOutputCols(
			new String[] {"f0", "f1", "f2", "f3", "f4", "f5"}).transform((StreamOperator)getData(false)).print();
		StreamOperator.execute();
	}

}
