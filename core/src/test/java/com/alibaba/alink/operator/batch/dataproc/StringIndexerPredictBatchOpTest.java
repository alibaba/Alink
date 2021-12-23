package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringIndexerPredictBatchOpTest {
	@Test
	public void testStringIndexerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setStringOrderType("frequency_asc");
		BatchOperator <?> predictor = new StringIndexerPredictBatchOp().setSelectedCol("f0").setOutputCol(
			"f0_indexed");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		predictor.linkFrom(model, data).print();
	}
}