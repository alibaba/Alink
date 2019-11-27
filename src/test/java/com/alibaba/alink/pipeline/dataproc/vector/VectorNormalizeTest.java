package com.alibaba.alink.pipeline.dataproc.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VectorNormalizeTest {
	AlgoOperator getData(boolean isBatch) {
		TableSchema schema = new TableSchema(
			new String[] {"id", "c0", "c1", "c2"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.STRING}
		);

		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(new Object[] {"0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0", "1 4 6 8"}));
		rows.add(Row.of(new Object[] {"1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0", "1 4 6 8"}));
		rows.add(Row.of(new Object[] {"2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0", "1 4 6 8"}));

		if (isBatch) {
			return new MemSourceBatchOp(rows, schema);
		} else {
			return new MemSourceStreamOp(rows, schema);
		}
	}

	@Test
	public void pipelineBatchTest() throws Exception {
		BatchOperator res = new VectorNormalizer().setP(2.0)
			.setOutputCol("pm")
			.setSelectedCol("c0").transform((BatchOperator)getData(true));
		List rows = res.getDataSet().collect();
		HashMap<String, Vector> map = new HashMap<String, Vector>();
		map.put((String) ((Row) rows.get(0)).getField(0), VectorUtil.getVector(((Row) rows.get(0)).getField(4)));
		map.put((String) ((Row) rows.get(1)).getField(0), VectorUtil.getVector(((Row) rows.get(1)).getField(4)));
		map.put((String) ((Row) rows.get(2)).getField(0), VectorUtil.getVector(((Row) rows.get(2)).getField(4)));
		assertEquals(map.get("0"),
			VectorUtil.getVector("$6$1:0.35640489924669927 2:0.5346073488700489 5:0.7662705333804034"));
		assertEquals(map.get("1"),
			VectorUtil.getVector("$8$1:0.35640489924669927 2:0.5346073488700489 7:0.7662705333804034"));
		assertEquals(map.get("2"),
			VectorUtil.getVector("$8$1:0.35640489924669927 2:0.5346073488700489 7:0.7662705333804034"));

	}

	@Test
	public void pipelineStreamTest() throws Exception {
		new VectorNormalizer().setP(2.0)
			.setOutputCol("pm")
			.setSelectedCol("c0").transform((StreamOperator)getData(false)).print();
		StreamOperator.execute();
	}
}