package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeMultiIndexerStringPredictBatchOpTest {
	@Test
	public void testHugeMultiStringIndexerPredict() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1L, "football", "apple"),
			Row.of(2L, "football", "apple"),
			Row.of(3L, "football", "apple"),
			Row.of(4L, "basketball", "apple"),
			Row.of(5L, "basketball", "apple"),
			Row.of(6L, "tennis", "pair"),
			Row.of(7L, "tennis", "pair"),
			Row.of(8L, "pingpang", "banana"),
			Row.of(9L, "pingpang", "banana"),
			Row.of(0L, "baseball", "banana")
		);
		// baseball 1
		// basketball,pair,tennis,pingpang 2
		// footbal,banana 3
		// apple 5
		BatchOperator <?> data = new MemSourceBatchOp(df, "id long,f0 string,f1 string");
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);

		BatchOperator <?> predictor = new HugeMultiStringIndexerPredictBatchOp().setSelectedCols("f0", "f1");
		BatchOperator result = predictor.linkFrom(model, data);
		result.lazyPrint(10);

		BatchOperator <?> stringPredictor = new HugeMultiIndexerStringPredictBatchOp().setSelectedCols("f0", "f1")
			.setOutputCols("f0_source", "f1_source");
		stringPredictor.linkFrom(model, result).print();
	}
}
