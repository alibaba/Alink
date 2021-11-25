package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultiStringIndexerPredictBatchOpTest {
	@Test
	public void testMultiStringIndexerPredict() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		// baseball 1
		// basketball,pair,tennis,pingpang 2
		// footbal,banana 3
		// apple 5
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);

		BatchOperator <?> predictor = new MultiStringIndexerPredictBatchOp().setSelectedCols("f0", "f1").setOutputCols(
			"f0_indexed", "f1_indexed");
		predictor.linkFrom(model, data).print();
	}
}
