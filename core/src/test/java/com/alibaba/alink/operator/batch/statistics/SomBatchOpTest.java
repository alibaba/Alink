package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.SomBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SomBatchOpTest extends AlinkTestBase {
	static void createCube(List <Row> rows, double x, double y, int cnt) {
		Random random = new Random();
		for (int i = 0; i < cnt; i++) {
			rows.add(Row.of(x + random.nextDouble(), y + random.nextDouble()));
		}
	}

	/**
	 * @throws Exception
	 */
	@Test
	public void testSom() throws Exception {
		List <Row> rows = new ArrayList <>();
		final int cnt = 128;

		createCube(rows, 0., 0., cnt);
		createCube(rows, 4., 4., cnt);
		createCube(rows, 0., 2., cnt);

		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"x", "y"});

		data = data.link(new VectorAssemblerBatchOp()
			.setSelectedCols(new String[] {"x", "y"})
			.setOutputCol("v"))
			.link(new SomBatchOp()
				.setXdim(4)
				.setYdim(4)
				.setVdim(2)
				.setSigma(1.0)
				.setNumIters(10)
				.setEvaluation(true)
				.setVectorCol("v"));
		data.print();
	}
}