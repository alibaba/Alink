package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeStringIndexerPredictBatchOpTest extends AlinkTestBase {
	@Test
	public void testStringIndexerPredictBatchOp() throws Exception {
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
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("frequency_asc");
		BatchOperator <?> predictor = new HugeStringIndexerPredictBatchOp().setSelectedCols("f0", "f1")
			.setOutputCols("f0_indexed", "f1_indexed");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		BatchOperator result = predictor.linkFrom(model, data);
		result.print();
	}
}
