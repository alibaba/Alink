package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SosBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("2. 2."));
		rows.add(Row.of("1. 1."));
		rows.add(Row.of("1. 2."));
		rows.add(Row.of("2. 1."));
		rows.add(Row.of("5. 5."));

		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"x"});

		BatchOperator sos = new SosBatchOp()
			.setVectorCol("x")
			.setPredictionCol("score")
			.setPerplexity(2.0);
		data.link(sos).print();
		Assert.assertEquals(data.link(sos).count(), 5);
	}
}