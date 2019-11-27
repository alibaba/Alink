package com.alibaba.alink.operator.batch.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.common.utils.httpsrc.Iris;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SosBatchOpTest {
	@Test
	public void test() throws Exception {
		List<Row> rows = new ArrayList<>();
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

		Assert.assertEquals(data.link(sos).count(), 5);
	}
}